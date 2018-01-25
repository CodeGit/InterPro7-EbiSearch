from db import DBConnection
import json
from os import path
from datetime import date

class InterProData:
    """Database interaction class"""

    def __init__(self, config):
        self.config = config
        self.connection = DBConnection(self.config["mysql"])

    def _runQuery(self, query, connection):
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
        if 'mysql_cache' in self.config:
            if not nocache and path.isfile(self.config['mysql_cache']):
                file = open(self.config['mysql_cache'], "r")
                results = json.load(file)
            else:
                results = self.getDBResults()
                file = open(self.config['mysql_cache'], "w")
                file.write(json.dumps(results, indent=4, sort_keys=True))

        return results

    def getDBResults(self, filter=None):
        """Get a list of all entries"""
        query = """select   accession, 
                            type, 
                            name, 
                            short_name, 
                            name, 
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

    def resultsToEntries(self, results):
        entries = []
        for result in results:
            entry = {
                "fields": [],
                'cross_references': []
            }

            if result["accession"]:
                entry["fields"].append(self.createField("id", result["accession"]))
            if result["name"]:
                entry["fields"].append(self.createField("name", result["name"]))
            if result["short_name"]:
                entry["fields"].append(self.createField("short_name", result["short_name"]))
            if result["description"]:
                entry["fields"].append(self.createField("description", result["description"]))
            if result["entry_date"]:
                entry["fields"].append(self.createField("created", result["entry_date"]))

            literature = json.loads(result['literature'])
            for paper in literature:
                if 'PMID' in literature[paper]:
                    entry['cross_references'].append(self.createCrossRef('PUBMED', literature[paper]['PMID']))
            goTerms = json.loads(result['go_terms'])
            for term in goTerms:
                entry['cross_references'].append(self.createCrossRef('GO', term['identifier']))

            entries.append(entry)
            entryObj = {
                'name': "InterPro 7",
                "release": "1",
            }
            entryObj["release_date"] = date.today().strftime("%Y-%m-%d")
            entryObj['entries'] = entries
            entryObj['entry_count'] = len(entries)
        return entryObj

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