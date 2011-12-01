"""Initialisation for the searchify app.

Despite the name, this file doesn't actually contain any models. It's just used
to contain initialisation for the searchify app, which is called at Django
setup time.

"""

from django.conf import settings
from hooks import connect_signals
from index import autodiscover

if hasattr(settings, 'ENABLE_SEARCHIFY') and settings.ENABLE_SEARCHIFY:
    connect_signals()
    autodiscover()
