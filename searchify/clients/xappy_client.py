"""Searchify client using Xappy as a backend.

Status: dubious - use with caution.

"""

from django.conf import settings

import xappy

def ClientFactory(index):
    return XappyClient(index)

class XappyClient(object):
    """
    Simple wrapper around Xappy that makes my life a little easier.
    """
    Error = Exception
    type = "xappy"
    
    def __init__(self, index):
        self.dbname = index
        self.index = None

    def get_index(self):
        if not self.index:
            self.index = xappy.IndexerConnection(os.path.join(getattr(settings, 'XAPPY_DB_ROOT', ''), self.dbname))
        return self.index
    
    def create(self, fields, reopen=False, overwrite=False):
        idx = self.get_index()
        for name, config in fields.items():
            # config format:
            #
            # store (bool, STORE_CONTEXT action)
            # sortable (bool, SORTABLE, COLLAPSE, WEIGHT actions)
            # type (text, geo, date, float)
            # freetext (dict containing INDEX_FREETEXT options weight, language, stop, spell, nopos, allow_field_specific, search_by_default)
            # exacttext (alternative to freetext)
            # facet (bool, FACET, type text->string, float->float)
            # geo (bool)
            # image (dict, approach -> imgseek)
            # colour / color (bool)
            # If no other actions apply, freetext will happen automatically.
            
            type = config.get('type', 'text')
            
            processed_action = False
            if config.get('store', False):
                idx.add_field_action(name, xappy.FieldActions.STORE_CONTENT)
                processed_action = True
            if config.get('sortable', False):
                if type=='float' and config.has_key('ranges'):
                    idx.add_field_action(name, xappy.FieldActions.SORTABLE, type=type, ranges=config['ranges'])
                else:
                    idx.add_field_action(name, xappy.FieldActions.SORTABLE, type=type)
                idx.add_field_action(name, xappy.FieldActions.COLLAPSE)
                if type=='float':
                    idx.add_field_action(name, xappy.FieldActions.WEIGHT)
                processed_action = True
            if config.get('facet', False):
                if type=='float':
                    t = 'float'
                else:
                    t = 'string'
                idx.add_field_action(name, xappy.FieldActions.FACET, type=t)
                processed_action = True
            if config.get('geo', False):
                idx.add_field_action(name, xappy.FieldActions.GEOLOCATION)
                processed_action = True
            if config.get('image', False):
                if config['image'].get('approach')=='imgseek':
                    idx.add_field_action(name, xappy.FieldActions.IMGSEEK)
                    processed_action = True
            if config.get('colour', False) or config.get('color', False):
                idx.add_field_action(name, xappy.FieldActions.COLOUR)
                processed_action = True
            if config.get('exacttext', False):
                idx.add_field_action(name, xappy.FieldActions.INDEX_EXACT)
            elif config.get('freetext', False) or not processed_action:
                idx.add_field_action(name, xappy.FieldActions.INDEX_FREETEXT, **config.get('freetext', {}))

    def add(self, fielddata, docid):
        doc = xappy.UnprocessedDocument()
        for name, values in fielddata.items():
            if type(values) in (str, unicode):
                values = [values]
            for v in values:
                doc.fields.append(xappy.Field(name, v))
        doc.id = docid
        self.get_index().add(doc)
        
    def delete(self, docid):
        self.get_index().delete(docid)
        
    def flush(self):
        self.get_index().flush()
         
    def close(self):
        self.flush()
        self.get_index().close()
        self.index = None
