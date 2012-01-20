"""Client for elasticsearch, using pyes.

To enable this client in the django config, set SEARCHIFY_ENGINE to 'pyes', and
set ENABLE_SEARCHIFY to True.

This client uses two settings from the django config:

 - `PYES_ADDRESS` (required, a string): The address to contact to talk to
   elasticsearch.  Typically, this will be of the form 'hostname:port';
   elasticsearch usually listens on port 9200 for the HTTP transport, or on
   port 9500 for the thrift transport.

 - `PYES_PERSONAL_PREFIX` (optional, a string, defaults to ""). A prefix which
   will be added to all indexnames used.  This can be used to allow multiple
   users to use the same elasticsearch cluster without interfering with each
   other - this is particularly useful in development environments where you
   don't wish to require all users to run an elasticsearch server.

"""

import copy
from django.conf import settings
import searchify
import pyes
import pyes.exceptions

personal_prefix = getattr(settings, "PYES_PERSONAL_PREFIX", "")

class Client(object):
    """Client to talk to the pyes backend.

    This can be used to get an indexer for performing index actions on a
    specific index, or to get a searcher for performing search actions (either
    on a specific index, or a general searcher which searches all known
    indexes).

    This reads the necessary settings from the Django config.

    """
    def __init__(self):
        self.conn = pyes.ES(settings.PYES_ADDRESS, timeout=30)

    def get_indexer(self, indexname):
        """Get an indexer for a given index name.

        """
        return IndexerClient(self, indexname)

    def get_searcher(self, indexname):
        """Get a searcher for a given index name.

        """
        return PyesSearchQS(self, indexname)

    def all_indexes(self):
        """Return a dict with information on all known indexes.

        (If a personal_prefix was supplied, only indexes undex this prefix are
        shown, and the prefix is stripped from them.)

        Returns a dict, keyed by indexname, in which the values are a dict with
        the following properties:

         - num_docs: number of docs in the index.
         - alias_for: if this indexname is an alias, a list of the indexes it
           is an alias for.

        """
        indices = self.conn.get_indices(include_aliases=True)
        res = {}
        for index, info in indices.iteritems():
            if not index.startswith(personal_prefix):
                continue
            index = index[len(personal_prefix):]

            newinfo = dict(num_docs = info['num_docs'])
            if 'alias_for' in info:
                aliases = []
                for alias in info['alias_for']:
                    if alias.startswith(personal_prefix):
                        alias = alias[len(personal_prefix):]
                    aliases.append(alias)
                newinfo['alias_for'] = aliases

            res[index] = newinfo
        return res

    def get_alias(self, alias):
        """Get a list of the indexes pointed to by an alias.

        Returns an empty list if the alias does not exist.

        """
        try:
            result = []
            for indexname in self.conn.get_alias(personal_prefix + alias):
                if indexname.startswith(personal_prefix):
                    indexname = indexname[len(personal_prefix):]
                result.append(indexname)
            return result
        except pyes.exceptions.IndexMissingException:
            return []

    def delete_index(self, indexname):
        """Delete the named index (or alias).

        If the index is not found, does not raise an error.

        """
        self.conn.delete_index_if_exists(personal_prefix + indexname)
        self.conn.set_alias(personal_prefix + indexname, [])

    def set_alias(self, alias, indexname):
        """Set an alias to point to an index.

        """
        self.conn.set_alias(personal_prefix + alias, personal_prefix + indexname)

    def flush(self):
        """Flush all changes made by the client.

        This forces all bulk updates to be sent to elasticsearch, but doesn't
        force a "refresh", so it may take some time after this call for the
        updates to become searchable.

        """
        self.conn.flush()

    def close(self):
        """Close the client.

        """
        self.flush()
        self.conn.connection.close()
        self.conn = None


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
        self.client.conn.create_index(self._target_name, index_settings)

    def set_mapping(self, doc_type, fields):
        """Create the index, and add settings for a given doc_type, with
        specified field configuration.

        """
        try:
            self.client.conn.put_mapping(doc_type, dict(properties=fields), self._target_name)
        except pyes.exceptions.MapperParsingException, e:
            raise ValueError("Could not parse mapping supplied to index %r "
                             "for type %r: %s" % (self.indexname, doc_type, e))

    def get_mapping(self, doc_type):
        """Get the mapping for a given doc_type.

        """
        try:
            mapping = self.client.conn.get_mapping(doc_type, self._target_name)
        except pyes.exceptions.ElasticSearchException:
            return None
        if settings.ES_VERSION < 0.16:
            try:
                mapping = mapping[self._target_name]
            except KeyError:
                aliases = self.client.get_alias(self.indexname)
                if len(aliases) == 0:
                    return None
                try:
                    mapping = mapping[aliases[0]]
                except KeyError:
                    return None

        try:
            return mapping[doc_type]['properties']
        except KeyError:
            return None

    def add(self, doc, doc_type, docid):
        """Add a document of the specified doc_type and docid.

        Replaces any existing document of the same doc_type and docid.

        """
        self.client.conn.index(doc, self._target_name, doc_type=doc_type, id=docid, bulk=True)

    def delete(self, doc_type, docid):
        """Delete the document of given doc_type and docid.

        Doesn't report an error if the document wasn't found.

        """
        try:
            self.client.conn.delete(self._target_name, doc_type=doc_type, id=docid)
        except pyes.exceptions.NotFoundException:
            pass

    def flush(self):
        """Flush all changes made by the client.

        This forces all bulk updates to be sent to elasticsearch, but doesn't
        force a "refresh", so it may take some time after this call for the
        updates to become searchable.

        """
        self.client.flush()

class SearchQS(object):
    """A simple wrapper around a query and the parameters which will be used
    for a search, to allow a search to be built up easily.

    """
    # FIXME - this should really be backend independent, but the execute method
    # implemented here isn't yet.
    def __init__(self, client, indexname):
        self._client = client
        self._indexname = personal_prefix + indexname
        self._doc_types = set()

    def clone(self):
        """Clone method, used when chaining.

        """
        return copy.copy(self)

    def for_type(self, type):
        """Return a new SearchQS which searches only for a specific type.

        Multiple types may be specified by passing a sequence instead of a
        single string.

        Any previous types searched for by this SearchQS are dropped.

        """
        result = self.clone()
        result._doc_types = set()
        if isinstance(type, basestring):
            result._doc_types.add(type)
        else:
            for t in type:
                result._doc_types.add(type)
        return result

    def execute(self, **kwargs):
        """Perform the search, and return a result set object.

        """
        raise NotImplementedError("Subclasses should implement this")

class PyesSearchQS(SearchQS):
    """A client for building searches.

    """
    def __init__(self, client, indexname):
        super(PyesSearchQS, self).__init__(client, indexname)
        self._query = None
        self._facets = []
        self.query_params = {}

    def execution_type(self, type):
        """Set the query execution type.

        The default is set by elasticsearch, but in the 0.15 release is
        query_then_fetch, which runs the query on each shard without first
        sharing global statistics.

        An alternative is dfs_query_then_fetch, which first shares globabl
        statistics.  There are also _and_fetch variants, which return "size"
        results from each shard, to cut down on communication.

        """
        result = self.clone()
        assert type in ('query_and_fetch',
                        'query_then_fetch',
                        'dfs_query_and_fetch',
                        'dfs_query_then_fetch',
                       )
        self.query_params['search_type'] = type
        return result

    def add_facet(self, facet):
        self._facets.append(facet)

    def parse(self, query_string, *args, **kwargs):
        """Construct a search query by parsing user input.

        """
        result = self.clone()
        result._query = pyes.StringQuery(query_string, *args, **kwargs)
        return result

    def flt(self, fields, text, *args, **kwargs):
        result = self.clone()
        result._query = pyes.FuzzyLikeThisQuery(fields, text, *args, **kwargs)
        return result

    def text_query(self, query_string, *args, **kwargs):
        """Construct a text search query by parsing user input.

        """
        result = self.clone()
        result._query = pyes.TextQuery(query_string, *args, **kwargs)
        return result

    def field_parse(self, field, query_string, *args, **kwargs):
        """Construct a search in a field, by parsing user input.

        """
        result = self.clone()
        q = pyes.FieldQuery()
        q.add(field, query_string, *args, **kwargs)
        result._query = q
        return result

    def custom_score(self, script, *args, **kwargs):
        """Apply a custom weight to the query.

        """
        result = self.clone()
        result._query = pyes.CustomScoreQuery(result._query, script,
                                              *args, **kwargs)
        return result

    def dis_max(self, queries, **kwargs):
        result = self.clone()
        _queries = []
        for q in queries:
            if isinstance(q, PyesSearchQS):
                _queries.append(q._query)
            else:
                _queries.append(q)
        result._query = pyes.query.DisMaxQuery(_queries, **kwargs)
        return result

    def execute(self, **kwargs):
        search = self._query.search(**kwargs)
        search.facet.facets = self._facets
        response = self._client.conn.search(search,
                                            (self._indexname,),
                                            tuple(sorted(self._doc_types)),
                                            **self.query_params)
        return SearchResultSet(response, search)

class SearchResult(object):
    """An individual search result.

    """
    def __init__(self, type, pk, score, hit):
        self.type = type
        self.pk = pk
        self.score = score
        self.hit = hit

class SearchResultSet(object):
    def __init__(self, response, search):
        self.start_rank = search.start
        self.requested_size = search.size
        self.response = response
        self.search = search
        try:
            hits = response['hits']
        except KeyError:
            hits = {}
        self._hits = hits.get('hits', [])
        try:
            facets = response['facets']
        except KeyError:
            facets = {}
        self._facets = facets
        self.count = hits.get('total', 0)
        self.more_matches = (self.count > self.start_rank + self.requested_size)

    def __len__(self):
        """Get the number of result items in this result set.

        """
        return len(self._hits)

    @property
    def results(self):
        for hit in self._hits:
            pk = long(hit['_id'])
            type = searchify.utils.lookup_model(hit.get('_type'))
            if type is None:
                raise Exception("Model %s not found" % hit.get('_type'))
            yield SearchResult(type, pk, hit.get('_score', 0), hit)
