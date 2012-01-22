"""
Searchify works by having a suitable subclass of Searchable available for each model we care about.

This can be set up by one of:

 - Place a nested class in the model class called `Indexer`. (A single instance
   will be created and registered for the model.)
 - Call `register_indexer` directly with an instance.

"""

import copy
import sys
import time

from django.db import models
from django.conf import settings

import search # for make_searcher
from clients import Client
from utils import get_indexer, get_searcher, get_typename_from_object

client = Client()

class SearchifyOptions(object):
    def __init__(self, indexer=None):
        self.indexer = indexer

# Map search index -> list of models which have an indexer for that index.
_index_models = {} 

def register_indexer(model, indexer):
    """Register an indexer on a model.

    This stores the indexer in a property on the model.
    """
    
    if not hasattr(settings, 'ENABLE_SEARCHIFY') or not settings.ENABLE_SEARCHIFY:
        return

    if not hasattr(model, '_searchify'):
        model._searchify = SearchifyOptions()
    model._searchify.indexer = indexer
    indexer.model = model
    if indexer.index:
        _index_models.setdefault(indexer.index, []).append(model)

        # indexer.managers is a list of attribute names (eg: ['objects']) for managers we want to
        # decorate
        for manager in indexer.managers:
            manager = getattr(model, manager)
            manager.query = indexer.make_searcher(manager)

_ensure_dbs_exist = True
def autodiscover(verbose=None, ensure_dbs_exist=None):
    """Automatically register all models with Indexer subclasses.

    Romp through the models, creating instances of their Indexer subclasses as
    required.

    This must be called, even if you've registered the indexers yourself, since
    it sets up the search databases.
    """

    # if we get called once with ensure_dbs_exist=False, then we don't want
    # to ensure them later. This is an incredible hack to avoid having to put
    # autodiscovery into urls.py and still make it practical to have a simple
    # reindex command.
    #
    # Note we do it here so that commands that want to can defer model validation;
    # if we leave it until after the call to models.get_models() then model
    # validation will have happened and we'll have been called from searchify.models.
    global _ensure_dbs_exist
    if ensure_dbs_exist is None:
        ensure_dbs_exist = _ensure_dbs_exist
    _ensure_dbs_exist = ensure_dbs_exist

    if not hasattr(settings, 'ENABLE_SEARCHIFY') or not settings.ENABLE_SEARCHIFY:
        return

    for model in models.get_models():
        if not hasattr(model, '_searchify') and hasattr(model, 'Indexer'):
            # auto-register
            if verbose:
                verbose.write("Auto-creating indexer instance for class %s\n" %
                              model)
            register_indexer(model, model.Indexer(model))

    if not ensure_dbs_exist:
        return

    # Now loop through and ensure each model has a mapping.
    #for index, modellist in _index_models.items():
    #    for model in modellist:
    #        indexer = get_indexer(model)
    #        if indexer.get_current_mapping() is None:
    #            print >>sys.stderr, ("Mapping not stored for %r - need to run "
    #                        "reindex command" % indexer.get_typename(model))
    #            del _index_models[index]
    #            break

def reindex(indices):
    """Reindex the named indices, or all indices if none are named.

    The index is rebuilt from scratch with a new suffix, and the alias is then
    changed to point to the new index, so existing searchers should not be
    disrupted.
    """
    
    if not hasattr(settings, 'ENABLE_SEARCHIFY') or not settings.ENABLE_SEARCHIFY:
        return
    
    suffix = '_' + hex(int(time.time()))[2:]
    if not indices:
        indices = _index_models.keys()
    for indexname in indices:
        reindex_index(indexname, suffix)

def reindex_index(indexname, suffix):
    """Reindex a named index.
    """
    
    if not hasattr(settings, 'ENABLE_SEARCHIFY') or not settings.ENABLE_SEARCHIFY:
        return
    
    models = _index_models.get(indexname, None)
    if models is None:
        raise KeyError("Index %r is not known" % indexname)
    try:

        # Get the index-wide settings.
        index_settings = {}
        def merge_dicts(path, a, b):
            for (k, v) in b.iteritems():
                if k not in a:
                    a[k] = v
                    continue
                if isinstance(v, dict):
                    merge_dicts('%s.%s' % (path, k), a[k], v)
                    continue
                if a[k] == v:
                    continue
                raise ValueError("Conflicting values in index_settings (at %s)" % path[1:])
        for model in models:
            indexer = get_indexer(model)
            merge_dicts('.', index_settings, indexer.index_settings)

        created = False
        for model in models:
            print "Indexing %s to %s, using suffix %s" % (model, indexname, suffix)
            indexer = get_indexer(model)
            try:
                indexer.client.set_suffix(suffix)
                if not created:
                    #print "Creating index with settings %r" % index_settings
                    indexer.client.create_index(index_settings)
                    created = True
                indexer.apply_mapping()
                indexer.index_all(with_cascade=False)
            finally:
                indexer.client.set_suffix()
            indexer.client.flush()

        # Get the old value of the alias.
        try:
            old_index = client.get_alias(indexname)[0]
        except IndexError:
            old_index = None
        if old_index == indexname:
            # Old index wasn't an alias; we have to delete it and then set the
            # new alias for it.
            print "Warning: no alias in use, so must delete in-use index"
            old_index = None
            client.delete_index(indexname)
        print "Setting alias to make new index live"
        client.set_alias(indexname, indexname + suffix)
    except:
        try:
            client.delete_index(indexname + suffix)
        except Exception:
            # Ignore any normal exceptions, so we report the original error.
            pass
        raise
    if old_index:
        print "Removing old index: %s" % old_index
        client.delete_index(old_index)

class Indexer(object):
    """Main indexer superclass, controlling search indexing for a model.

    Most of the indexing behaviour is in here (except for the index clients).

    Typically you won't have to do much here, just subclass and set index,
    fields and perhaps cascades. However you can override any part of the
    indexing process if needed.

    """

    # index is the name of the index that this model should be indexed to.
    # The default is None, meaning that the model will not be indexed.
    index = None

    # Fields is a list of fields to be indexed.
    index_settings = {} # A dictionary of engine specific index-level settings.
    fields = []
    cascades = [] # no cascades
    managers = [] # don't create searcher by default (still pondering details, and searchers unported to class approach)
    defaults = {}

    def __init__(self, model):
        self.model = model
        if self.index:
            self.client = client.get_indexer(self.index)

    def reindex_on_cascade(self, cascade_from, cascade_to):
        """
        Should we reindex cascade_to when we've just reindexed cascade_from?
        Called on the indexer for cascade_to.
        """

        return True

    def should_be_in_index(self, instance):
        """
        Should we be in the index, based on the instance's current state?

        (For instance, this might want to return `not self.deleted`.)
        """

        return True

    def get_searcher(self):
        return self.client.get_searcher()

    def index_all(self, with_cascade=True):
        """Index or reindex all the instances of this model.

        If with_cascade is True, the cascade of instances depending on this
        instance will also be traversed, to update any search data built from
        these instances.

        """
        from django.db import connection
        for inst in self.model.objects.all():
            self.index_instance(inst, with_cascade)
            del inst
            connection.queries = []
        self.client.flush()

    def index_instance(self, instance, with_cascade=True):
        """Index or reindex an instance.

        If with_cascade is True, the cascade of instances depending on this
        instance will also be traversed, to update any search data built from
        these instances.

        """
        if self.index:
            if not self.should_be_in_index(instance):
                self.client.delete(self.get_typename(instance),
                                   self.get_docid(instance))
            else:
                dret = self.get_index_data(instance)
                if dret is not None:
                    (doc_type, docid, fielddata) = dret
                    self.client.add(fielddata, doc_type=doc_type, docid=docid)
        if with_cascade:
            self.cascade(instance)
        if self.index:
            self.client.flush()

    def cascade(self, instance):
        """Cascade the index from this instance to others that depend on it.

        This causes index_instance() to be called on each instance that depends
        on the instance supplied.

        """
        for descriptor in self.cascades:
            cascade_inst = None
            # find the instance we're being told to cascade the reindex onto
            try:
                if callable(descriptor):
                    cascade_inst = descriptor(instance)
                elif isinstance(descriptor, str):
                    cascade_inst = getattr(instance, descriptor)
            except:
                cascade_inst = None
            # if we found one, check if it's searchable, check if it
            # wants to accept the cascade, and if so, reindex it
            if cascade_inst:
                # If it's not an iterable already, make it into one
                if not hasattr(cascade_inst, '__iter__'):
                    cascade_insts = [cascade_inst]
                else:
                    cascade_insts = cascade_inst
                for cascade_inst in cascade_insts:
                    indexer = get_indexer(cascade_inst)
                    if indexer and indexer.reindex_on_cascade(instance, cascade_inst):
                        indexer.index_instance(cascade_inst, with_cascade=False)

    def delete(self, instance):
        """Delete an instance from the (relevant) search index.

        """

        if self.index:
            self.client.delete(self.get_typename(instance),
                               self.get_docid(instance))
            self.client.flush()

    def get_typename(self, instance):
        """Generate a type name for use in the search database.

        Default is in the format '<app_label>.<object_name>'

        """
        return get_typename_from_object(instance)

    def get_docid(self, instance):
        """Generate a docid for use as a search database identifier.

        Default is in the format ('<app_label>.<object_name>', '<pk>').

        """
        return '%s' % (instance.pk, )

    def get_index_data(self, instance):
        """Get the data to be indexed for an instance.

        Given a Django model instance, return a unique identifier and a
        dictionary of search fields mapping to lists of data, or None.

        """
        if not self.fields:
            return None

        outfields = {}

        for field in self.fields:
            (django_field_list, index_fieldname, index_config) = self.get_details(field)
            # print "indexing %s (%s)" % (instance, index_fieldname,)
            interim_data = map(lambda x: self.get_field_input(instance, x), django_field_list)
            # print '>>>' + str(interim_data)
            outfields[index_fieldname] = reduce(lambda x,y: list(x) + list(y), interim_data)

        return (self.get_typename(instance), self.get_docid(instance),
                outfields)


    def get_details(self, field):
        """
        Return (django_field_list, search field name, config) for a particular search field (which is a string, dict or possibly callable).

        Performs auto-generation of search field names as needed (generally it's not advised not to do this with a plain callable; use a
        dictionary, which makes things more clear).

        If the field is a dictionary, it will have:
            django_fields   list of django fields / callables
            field_name      single search field to index into
            config          additional config options pass through to indexing client on init

        If not, then django_fields is a list of it, and field_name is generated from:
            field is str    letters of field (eg: my_field -> myfield)
            field is callable
                            field(None)

        (Note that all these fields are in self.fields.)
        """

        if type(field) is dict:
            django_field_list = field['django_fields']
            index_fieldname = field.get('field_name')
        else:
            django_field_list = [field]
            index_fieldname = None

        if index_fieldname == None:
            if isinstance(django_field_list[0], str):
                field_specific_name = django_field_list[0]
            elif callable(django_field_list[0]):
                field_specific_name = django_field_list[0](None)
            index_fieldname = filter(lambda x: x.isalpha(), field_specific_name)

        if type(field) is dict:
            return (django_field_list, index_fieldname, field.get('config', {}))
        else:
            return (django_field_list, index_fieldname, {})


    def get_field_input(self, instance, django_field):
        """
        Given a single Django field descriptor (string or callable), generate a list of data to input to the search field.

        Converters allow Django ORM types to be modified automatically (eg: returning DateTimeField in a useful format).
        Currently, converters are embedded here, which isn't helpful.
        """

        # must return an iterable; django_field is str (name) or callable
        if isinstance(django_field, str):
            #print 'trying as str'
            #print '.name = %s' % instance.name
            #print 'getattr(,"name") = %s' % getattr(instance, 'name')
            val = getattr(instance, django_field)

            def datetime_converter(d):
                return unicode(d.date())

            # FIXME: converters should be on the class, not embedded in this method
            converters = { models.DateTimeField: datetime_converter }
            field_type = instance._meta.get_field(django_field).__class__
            if val == None:
                return []
            if field_type in converters:
                val = converters[field_type](val)
            else:
                val = unicode(val)
            return [val]
        elif callable(django_field):
            return django_field(instance)
        else:
            return []

    def get_configuration(self):
        """Get the configuration for this indexer, by looking at self.fields.

        """
        fields = {}
        for field in self.fields:
            config = copy.deepcopy(self.defaults)
            (_, search_fieldname, field_config) = self.get_details(field)
            config.update(field_config)
            fields[search_fieldname] = config
        return fields

    def get_current_mapping(self):
        """Get the current mapping for this indexer used by the search engine.

        """
        typename = self.get_typename(self.model)
        return self.client.get_mapping(typename)

    def apply_mapping(self):
        """Apply the configuration for this indexer to the search engine.

        """
        mapping = self.get_configuration()
        typename = self.get_typename(self.model)
        self.client.set_mapping(typename, mapping)

    def make_searcher(self, manager):
        """Make a searcher for the given manager.

        """
        return search.make_searcher(manager, self.model)
