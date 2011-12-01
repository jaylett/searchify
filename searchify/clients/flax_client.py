"""Searchify client using Flax as a backend.

Status: dubious - use with caution.

"""

from django.conf import settings
from flax.searchclient import Client, FlaxError

def ClientFactory(dbname):
    personal_prefix = getattr(settings, "FLAX_PERSONAL_PREFIX", "")
    return FlaxClient(personal_prefix + dbname, settings.FLAX_BASE_URL)

class FlaxClient:
    Error = FlaxError
    def __init__(self, dbname, url):
        self.client = Client(url)
        self.dbname = dbname
 
    def create(self, fields, reopen=False, overwrite=False):
        # print "making db"
        self.client.create_database(self.dbname, reopen=reopen, overwrite=overwrite)
        # print "done."
        schema = self.schema()
        for f,c in fields.items():
            # print "adding field %s (%s)" % (f, c)
            c = dict(c)
            if c.has_key("freetext") and c['freetext']:
                if c['freetext'].has_key("weight"):
                    c['freetext']['term_frequency_multiplier'] = c['freetext']['weight']
                    del c['freetext']['weight']
            schema.add_field(f, c)
        
    def schema(self):
        return self.client.schema(self.dbname)
        
    def add(self, doc, docid=None):
        db = self.client.db(self.dbname)
        # print "adding doc",
        ret = db.add_document(doc, docid)
        # print "done."
        return ret
        
    def search(self, query, query_filter=None, start=0, end=10):
        if filter:
            return self.client.db(self.dbname).search_structured(query_any=query, filter=query_filter, start_rank=start, end_rank=end)
        else:
            return self.client.db(self.dbname).search_simple(query, start, end)

    def get_searcher(self):
        """
        Return some sort of useful searching object.
        """
        return self.client.db(self.dbname)
        
    def delete(self, uid):
        db = self.client.db(self.dbname)
        ret = db.delete_document(uid)
        return ret
            
    def flush(self):
        db = self.client.db(self.dbname)
        db.flush()
            
    def close(self):
        self.flush()
        self.client.close()
        self.client = None
