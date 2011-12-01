# search-specific pieces

def make_searcher(manager, model):
    index = get_index(model)
    if not index:
        return None

    client = get_client(index)
    def search(query=None, start=None, end=None, query_filter=None):
        if start==None and end==None:
            # new-style interface. Return an object which responds to slicing and is iterable.
            class SearchResultSet:
                def __init__(self, query, query_filter=None):
                    self.query = query
                    self.query_filter = query_filter
                    self.position = None
                    self.smallest_page = 10
                    # the following are filled out as needed
                    self._results = None # just the current page of results (but contains useful metadata also)
                    self.results = {} # store of index -> decorated Django instance

                def __repr__(self):
                    return "<SearchResultSet()>"

                def __getattr__(self, key):
                    # print "__getattr__(%s)" % key
                    """Provide .-access to aspects of the result. eg: q.doc_count (providing the search provider returns doc_count)."""
                    self._ensure_results()
                    return getattr(self._results, key)
                    
                def _ensure_results(self, start=None, end=None):
                    # print "_ensure_results(%s,%s)" % (start, end)
                    """
                    Call this before any operation to ensure that we've got some useful results available.
                    If called with start=None, then ensure that the result at self.position is available.
                    If not, ensure that the result at [start] or results at [start:end] are available.
                    """
                    if start==None:
                        start=0
                    if end==None:
                        end=start+1
                    # now reduce [start:end] to the single smallest range that we don't already have
                    while self.results.has_key(start) and start < end:
                        start += 1
                    while self.results.has_key(end) and start < end:
                        end -= 1
                    if start==end:
                        # we have everything we need already
                        return
                    if end-start < self.smallest_page:
                        end = start + self.smallest_page
                    
                    self._results = client.search(self.query, self.query_filter, start, end)
                    result_ids = []
                    match_details = {}
                    for item in self._results.results:
                        search_id = item.docid
                        database_name, model_key, django_id = search_id.split('.')
                        if model_key != model.__name__:
                            # FIXME: not right!
                            # Either we need to get this right, or we could filter the query through a boolean to restrict to
                            # this model in the first place. The latter would work better, but requires some thought. (In particular,
                            # semi-constructing queries like this unsettles Richard, so there's probably a reason to avoid it.)
                            #continue
                            pass
                        result_ids.append(long(django_id))
                        match_details[django_id] = item
                    bulk = manager.in_bulk(result_ids)

                    for key, obj in bulk.items():
                        # From Flax we get: data (dict of field->data pairs), id, rank
                        # We only really care about rank at this stage, as we've pulled out the object.
                        if hasattr(model.Searchable, 'match_details_attribute'):
                            match_attr = model.Searchable.match_details_attribute
                        else:
                            match_attr = 'match'
                        if match_attr is not None:
                            setattr(obj, match_attr, match_details[str(obj.pk)])
                        self.results[start] = obj
                        start += 1
                
                def __iter__(self):
                    return self
                    
                def next(self):
                    # print "next()"
                    self._ensure_results()
                    ret = self[self.position]
                    self.position += 1
                    return ret
                    
                def __len__(self):
                    # print "__len__()"
                    self._ensure_results()
                    # this is perhaps not the ideal solution, but should work in general
                    return self.matches_upper_bound
                    
                def __getslice__(self, start, end):
                    # print "__getslice__(%s, %s)" % (start, end)
                    self._ensure_results(start, end)
                    ret = []
                    try:
                        for idx in range(start, end):
                            ret.append(self.results[idx])
                    except KeyError:
                        # Slices fail silently
                        pass
                    return ret
                    
                def __getitem__(self, index):
                    # print "__getitem__(%s)" % index
                    self._ensure_results(index)
                    try:
                        return self.results[index]
                    except KeyError:
                        # Indexing fails noisily
                        raise IndexError('list index out of range')
            
            return SearchResultSet(query, query_filter)
        else:
            # old-style interface
            if start==None:
                start=0
            if end==None:
                end=10
        class QueryResult:
            def __init__(self, results):
                self.results = results
                search_ids = [ item.docid for item in results.results ]
                self._result_ids = []
                self._deets = {}
                for item in results.results:
                    search_id = item.docid
                    database_name, model_key, id = search_id.split('.')
                    if model_key != model.__name__:
                        # FIXME: not right!
                        pass
                    self._result_ids.append(long(id))
                    self._deets[id] = item
                self._bulk = manager.in_bulk(self._result_ids)

            # From Flax, we get: matches_lower_bound, matches_upper_bound, more_matches, matches_estimated, matches_human_readable_estimate
            def __getattr__(self, key):
                """Provide .-access to aspects of the result. eg: q.doc_count (providing the search provider returns doc_count)."""
                return getattr(self.results, key)

            def __len__(self):
                return len(self._result_ids)
            
            def __iter__(self):
                """
                Iterate over the results, in the order they were in the result set.
                Return a decorated object, ie the Django model instance with an extra attribute (default 'match') containing match details (you mostly care about .rank, if provided).
                """
                for key in self._result_ids:
                    obj = self._bulk[long(key)]
                    # From Flax we get: data (dict of field->data pairs), id, rank
                    # We only really care about rank at this stage, as we've pulled out the object.
                    if hasattr(model.Searchable, 'match_details_attribute'):
                        match_attr = model.Searchable.match_details_attribute
                    else:
                        match_attr = 'match'
                    if match_attr is not None:
                        setattr(obj, match_attr, self._deets[str(obj.pk)])
                    yield obj
        
        results = client.search(query, query_filter, start, end)
        return QueryResult(results)
    return search
