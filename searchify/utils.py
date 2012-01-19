"""Utility functions for searchify.

"""

from django.db import models
from django.conf import settings

if hasattr(settings, 'ENABLE_SEARCHIFY') and settings.ENABLE_SEARCHIFY:
    def get_searcher(model_or_instance):
        """Given a model or instance, find the searcher for it.

        """
        if not isinstance(model_or_instance, models.base.ModelBase):
            model = type(model_or_instance)
        else:
            model = model_or_instance
        if not hasattr(model, '_searchify'):
            return None
        if not hasattr(model._searchify, 'searcher'):
            return None
        return model._searchify.searcher

    def get_indexer(model_or_instance):
        """Given a model or instance, find the indexer for it.

        """
        if not isinstance(model_or_instance, models.base.ModelBase):
            model = type(model_or_instance)
        else:
            model = model_or_instance
        if not hasattr(model, '_searchify'):
            return None
        if not hasattr(model._searchify, 'indexer'):
            return None
        return model._searchify.indexer
else:
    get_indexer = get_searcher = lambda x: None


def lookup_model(modeldesc):
    """Convert a packed docid into a Model.

    """
    try:
        (app_label, model_name) = modeldesc.rsplit("|", 1)
    except ValueError:
        return None
    return models.get_model(app_label, model_name)


def get_typename_from_object(instance):
    return '%s|%s' % (
        instance._meta.app_label, instance._meta.object_name,
    )
