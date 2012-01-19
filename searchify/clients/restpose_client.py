"""Client for restpose.

To enable this client, set SEARCHIFY_ENGINE to 'restpose' and
ENABLE_SEARCHIFY to True.

Further settings:

 - `RESTPOSE_URL`: restpose server you can read from

 - `RESTPOSE_MASTER_URL`: where to write to

 - `RESTPOSE_PERSONAL_PREFIX`: prefixes the collection names

"""

from django.conf import settings
import searchify
import restpose
import time
import restkit

personal_prefix = getattr(settings, "RESTPOSE_PERSONAL_PREFIX", "")

class Client(object):

    def __init__(self):
        self.read = restpose.Server(settings.RESTPOSE_URL)
        self.write = restpose.Server(
            getattr(settings, "RESTPOSE_MASTER_URL", settings.RESTPOSE_URL)
            )

    def get_indexer(self, indexname):
        return IndexerClient(self, indexname)

    def get_searcher(self, indexname):
        return self.read

    def all_indexes(self):
        res = {}
        for index in self.read.collections:
            coll = self.read.collection(index)
            status = coll.status
            info = {
                'num_docs': status['doc_count'],
                }
            res[index] = info
        return res

    def get_alias(self, alias):
        return []

    def delete_index(self, indexname):
        self.write.collection(indexname).delete()

    def set_alias(self, alias, indexname):
        raise NotImplementedError("oh no not now")

    def flush(self):
        pass

    def close(self):
        pass


class IndexerClient(object):
    def __init__(self, client, indexname):
        self.client = client
        self.indexname = indexname
        self.suffix = ''
        self._target_name = None
        self._set_target_name()

    def set_suffix(self, suffix=''):
        """Set a suffix to be appended to the index name for all subsequent
        operations.

        This is used during reindexing to direct all updates to a new index.

        """
        self.suffix = suffix
        self._set_target_name()

    def _set_target_name(self):
        self._target_name = personal_prefix + self.indexname + self.suffix

    def create_index(self, index_settings):
        coll = self.client.write.collection(self._target_name)
        coll.config = index_settings

    def set_fields(self, fields):
        coll = self.client.write.collection(self._target_name)
        config = coll.config
        config['fields'] = fields
        coll.config = config

    def add(self, doc, doc_type, docid):
        coll = self.client.write.collection(self._target_name)
        coll.add_doc(doc, doc_type=doc_type, doc_id=docid)

    def delete(self, doc_type, docid):
        coll = self.client.write.collection(self._target_name)
        coll.delete_doc(doc_type=doc_type, doc_id=docid)

    def flush(self):
        pass
