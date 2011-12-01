"""Hooks to ensure that the indexer is informed when an indexed instance
changes.

"""

from django.db.models.signals import post_save, pre_delete, post_delete

from index import get_indexer

def connect_signals():
    post_save.connect(index_hook)
    pre_delete.connect(delete_hook)

def index_hook(sender, **kwargs):
    instance = kwargs['instance']
    indexer = get_indexer(instance)
    if indexer:
        indexer.index_instance(instance)

def delete_hook(sender, **kwargs):
    instance = kwargs['instance']
    indexer = get_indexer(instance)
    if indexer:
        indexer.delete(instance)
        post_delete.connect(post_delete_hook(instance))

def post_delete_hook(instance):
    def hook(sender, **kwargs):
        if kwargs['instance'] == instance:
            indexer = get_indexer(instance)
            if indexer:
                indexer.cascade(instance)
            post_delete.disconnect(hook)
    return hook
