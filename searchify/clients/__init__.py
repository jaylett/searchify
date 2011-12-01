"""This module contains the clients which talk to search systems.

Currently there is decent support for pyes, and some old and flaky support for
Xappy and Flax.

"""

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

import os.path


def import_client(engine):
    """Import a lib.searchify client."""
    
    mod = __import__(engine + '_client', globals(), locals(),
                 fromlist=['Client'], level=1)
    return mod.Client


if hasattr(settings, 'ENABLE_SEARCHIFY') and settings.ENABLE_SEARCHIFY:
    engine = getattr(settings, "SEARCHIFY_ENGINE", None)
    if engine is None:
        raise ImproperlyConfigured('No engine configured for searchify: '
                                   'specify settings.SEARCHIFY_ENGINE')

    Client = import_client(engine)
else:
    Client = import_client('unconfigured')
