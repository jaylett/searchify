from django.core.management.base import BaseCommand, CommandError
import searchify
from optparse import make_option
import pprint

class Command(BaseCommand):
    args = '[<indexname> ...]'
    help = """Show searchify configuration for specified indices.

Shows configuration for all indicies if none specified.

    """.strip()

    def show_config(self, indices, verbose_out):
        searchify.autodiscover(verbose=verbose_out, ensure_dbs_exist=False)
        index_models = searchify.index._index_models
        if not indices:
            indices = index_models.keys()

        for indexname in indices:
            self.stdout.write("Configuration for index %r\n" % indexname)
            for model in index_models[indexname]:
                self.stdout.write("From model: %s\n" % model)

                indexer = searchify.utils.get_indexer(model)
                for field, config in sorted(indexer.get_configuration().items()):
                    self.stdout.write(" - %s:\n" % field)
                    for k, v in sorted(config.items()):
                        self.stdout.write("     %s: %s\n" % (k , v))

                if verbose_out:
                    verbose_out.write("Stored mapping:\n%s\n" %
                            pprint.pformat(indexer.get_current_mapping()))
                self.stdout.write("\n")

    def handle(self, *args, **kwargs):
        if kwargs.get('verbosity') == '2':
            verbose_out = self.stdout
        else:
            verbose_out = None
        self.show_config(args, verbose_out)
