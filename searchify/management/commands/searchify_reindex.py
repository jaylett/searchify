from django.core.management.base import BaseCommand, CommandError
import searchify
from optparse import make_option

class Command(BaseCommand):
    args = '[<indexname> ...]'
    help = """Reindex specified index.

Reindexes all indices if none specified.

This clears the specified index and rebuilds it from scratch.

In order to avoid causing search not to return appropriate results during the
reindexing, indexes are actually named with a suffix based on the creation
time, and an alias is set to point to this from the unsuffixed name.  This
alias is updated after the indexing completes, and then the old index is
deleted.

This means that searches will switch over the the new index only after a
successsful reindex.

    """.strip()

    requires_model_validation = False

    def __init__(self):
        super(Command, self).__init__()

    def handle(self, *args, **kwargs):
        """
        Note that we defer model validation until after we've run autodiscover.
        This means we can call with ensure_dbs_exist=False (which is sticky),
        so that we don't get an error for not having a mapping in the database
        for anything that hasn't been indexed yet.
        """

        searchify.autodiscover(ensure_dbs_exist=False)
        self.validate()
        searchify.reindex(args)
