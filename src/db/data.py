from db import DBConnection
import json
from os import path
from datetime import date
from urllib.parse import urljoin
from urllib.parse import urlunparse, urlparse
from urllib.request import urlopen
from random import shuffle
import re
import logging

logger = logging.getLogger(__name__)

class InterProData:
    """Database interaction class"""

    def __init__(self, config):
        self.config = config
        self.connection = DBConnection(self.config["mysql"])

    def _runQuery(self, query, connection):
        """General MySQL query runner"""
        with connection as c:
            cursor = c.cursor()
            cursor.execute(query)
            results = []
            result = cursor.fetchone()
            while result is not None:
                numCols = len(result)
                item = {}
                for col in range(0, numCols):
                    key = cursor.description[col][0]
                    value = result[col]
                    try:
                        #handle oracle clob datatypes
                        value = str(result[col].read())
                    except AttributeError:
                        pass
                    item[key] = value
                #item = {cursor.description[col][0]: result[col] for col in range(0, numCols)}
                results.append(item)
                result = cursor.fetchone()
        return results

    def getResults(self, nocache=False, filter=None):
        """Get results from database or stored json file"""
        if 'mysql_cache' in self.config:
            if not nocache and path.isfile(self.config['mysql_cache']):
                file = open(self.config['mysql_cache'], "r")
                results = json.load(file)
            else:
                results = self.getDBResults()
                file = open(self.config['mysql_cache'], "w")
                file.write(json.dumps(results, indent=4, sort_keys=True))
        shuffle(results)
        return results

    def getDBResults(self, filter=None):
        """Get a list of all entries and data from MySQL"""
        query = """select   accession, 
                            type, 
                            name, 
                            short_name, 
                            source_database,
                            member_databases,
                            integrated_id,
                            go_terms,
                            description,
                            literature,
                            hierarchy,
                            entry_date,
                            cross_references
                    from webfront_entry"""

        results = self._runQuery(query, self.connection)
        for result in results:
            #convert entry_date to string
            #hacky fix for date year being set to 0011 instead of 2011
            if result['entry_date'].year > 0 and result['entry_date'].year < 19:
                year = result['entry_date'].year + 2000
                result['entry_date'] = result['entry_date'].replace(year=year)
            date = result['entry_date'].strftime("%Y-%m-%d")
            result['entry_date'] = date
        return results

    def resultsToEntries(self, results, start=0, end=None, limit=None):
        """Convert MySQL data to Entry dictionary ready for conversion to EbiSearch JSON"""
        entries = []

        if limit != None:
            results = results[:limit]

        if end != None:
            results = results[start:end]

        for result in results:
            logger.info("Processing {}".format(result['accession']))
            entry = {
                "fields": [],
                'cross_references': []
            }
            #add the main fields
            entry["fields"].append(self.createField("id", result["accession"]))
            entry["fields"].append(self.createField("source_database", result["source_database"]))
            if result["name"]:
                entry["fields"].append(self.createField("name", result["name"]))
            if result["short_name"]:
                entry["fields"].append(self.createField("short_name", result["short_name"]))
            if result["description"]:
                descriptionParagraphs = json.loads(result["description"])
                descriptionText = " ".join(descriptionParagraphs)
                #clean up descriptions
                for pattern in [r"\[.*?\]", r"<.*?>", r"[\r|\n]+"]:
                    p = re.compile(pattern)
                    descriptionText = p.sub("", descriptionText)
                if len(descriptionText) > 0:
                    entry["fields"].append(self.createField("description", descriptionText))
            if result["entry_date"]:
                entry["fields"].append(self.createField("creation_date", result["entry_date"]))
            if result["type"]:
                entry["fields"].append(self.createField("type", result["type"]))
            #add all the cross references present in the entry table
            literature = json.loads(result['literature'])
            for paper in literature:
                if 'PMID' in literature[paper]:
                    entry['cross_references'].append(self.createCrossRef('PUBMED', literature[paper]['PMID']))
            goTerms = json.loads(result['go_terms'])
            for term in goTerms:
                entry['cross_references'].append(self.createCrossRef('GO', term['identifier']))
            memberDB = json.loads(result['member_databases'])
            for db in memberDB:
                entry['fields'].append(self.createField("contributing_database", db))
                for accession in memberDB[db]:
                    entry['cross_references'].append(self.createCrossRef(db, accession))
            crossReferences = json.loads(result['cross_references'])
            for db in crossReferences:
                dbName = db.split()[0]
                dbName = dbName.upper()
                for accession in crossReferences[db]:
                    entry['cross_references'].append(self.createCrossRef(dbName, accession))
            hierarchy = json.loads(result['hierarchy'])
            if 'children' in hierarchy:
                self.convertChildrenToCrossReferences(entry, hierarchy['children'])

            if result["integrated_id"]:
                entry['cross_references'].append(self.createCrossRef("INTERPRO", result["integrated_id"]))
            self.addElasticAnnotationToEntry(entry)
            entries.append(entry)

        #setup container object properties
        entryDict = {
            'name': "InterPro 7",
            "release": "1",
        }
        entryDict["release_date"] = date.today().strftime("%Y-%m-%d")
        entryDict['entries'] = entries
        entryDict['entry_count'] = len(entries)

        return entryDict

    def convertChildrenToCrossReferences(self, entry, children):
        for child in children:
            entry['cross_references'].append(self.createCrossRef("INTERPRO", child['accession']))
            if 'children' in child:
                self.convertChildrenToCrossReferences(entry, child['children'])

    def addElasticAnnotationToEntry(self, entry):
        scheme = self.config["elastic"]["scheme"]
        host = self.config["elastic"]["host"]
        basePath = self.config["elastic"]["path"]
        size = 20
        SEPARATOR = "____"

        accession = self.getFieldValue(entry['fields'], 'id').lower()
        escapedAccession = re.sub(r"(\:)", r"\\1", accession)
        sourceDB = self.getFieldValue(entry['fields'], 'source_database').lower()
        processedCount = 0
        hitCount = 1
        xrefs = set()
        logger.info("Annotating {}".format(accession))
        while processedCount < hitCount:
            query = "pretty&_source=true&from={0}&size={1}&default_operator=AND&q=entry_acc:{2}%20entry_db:{3}" \
                .format(processedCount, size, escapedAccession, sourceDB)
            try:
                url = urlunparse((scheme, host, basePath, None, query, None))
                response = urlopen(url)
            except Exception as e:
                logging.error("{0}: {1} URL:{2}".format(accession, e, url))
                break
            data = response.read().decode('utf-8')
            annotationData = json.loads(data)
            hits = annotationData['hits']['hits']
            for hit in hits:
                #ensure xrefs for a particular datatype are unique
                if '_source' in hit:
                    if 'protein_acc' in hit['_source'] and hit['_source']['protein_acc'] != None:
                        acc = SEPARATOR.join(["UNIPROT", hit['_source']['protein_acc']])
                        xrefs.add(acc)
                    if 'structure_acc' in hit['_source'] and hit['_source']['structure_acc'] != None:
                        acc = SEPARATOR.join(["PDBE", hit['_source']['structure_acc']])
                        xrefs.add(acc)
                    if 'set_acc' in hit['_source'] and hit['_source']['set_acc'] != None:
                        for set_acc in  hit['_source']['set_acc']:
                            acc = SEPARATOR.join([hit['_source']['set_db'].upper(), set_acc])
                            xrefs.add(acc)
                    if 'proteomes' in hit['_source'] and hit['_source']['proteomes'] != None:
                        for proteome in  hit['_source']['proteomes']:
                            acc = SEPARATOR.join(["PROTEOME", proteome])
                            xrefs.add(acc)
                    if 'lineage' in hit['_source'] and hit['_source']['lineage'] != None and len(hit['_source']['lineage']) > 0:
                        taxids = hit['_source']['lineage']
                        acc = SEPARATOR.join(["TAXONOMY", taxids[-1]])
                        xrefs.add(acc)
            for ref in xrefs:
                (dbName, accession) = ref.split(SEPARATOR)
                entry['cross_references'].append(self.createCrossRef(dbName, accession))
            hitCount = annotationData['hits']['total']
            processedCount += 1

    def addElasticAnnotation(self, entrySet):
        for entry in entrySet["entries"]:
            self.addElasticAnnotationToEntry(entry)

    def addAnnotation(self, entrySet, basepath, xrefName):
        host = self.config['api']['host']
        pageSize = 100

        for entry in entrySet["entries"]:
            accession = self.getFieldValue(entry['fields'], 'id')
            sourceDB = self.getFieldValue(entry['fields'], 'source_database')
            annotationCount = 0
            count = 1
            pageNum = 1

            while annotationCount < count:
                urlPath = "/".join([basepath, sourceDB, accession])
                urlPath += "/"
                try:
                    url = urlunparse(("https", host, urlPath, None, "page={0}&page_size={1}".format(pageNum, pageSize), None))
                    response = urlopen(url)
                except Exception as e:
                    logging.error("{0} [{1}]: {2} URL:{3}".format(accession, xrefName, e, url))
                    break
                data = response.read().decode('utf-8')
                annotationData = json.loads(data)
                for result in annotationData['results']:
                    annotationAccession = result['metadata']['accession']
                    if xrefName == None:
                        xrefName = result['metadata']['source_database'].upper()
                    entry['cross_references'].append(self.createCrossRef(xrefName, annotationAccession))
                    annotationCount += 1
                count = int(annotationData['count'])
                pageNum += 1
                logger.debug("Count={0} Payload={1} Total Processed={2} URL={3}".format(count, len(annotationData['results']), annotationCount, url))

    def getFieldValue(self, fields, name):
        for field in fields:
            if name == field['name']:
                return field['value']

    def createField(self, name, value):
        field = {
            "name": name,
            "value": str(value)
        }
        return field

    def createCrossRef(self, name, value):
        xref = {
            'dbname': name,
            'dbkey': str(value)
        }
        return xref