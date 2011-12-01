"""An empty class used to throw helpful exceptions when a search client isn't configured."""

from django.core.exceptions import ImproperlyConfigured

class Client(object):
    """Throws exceptions on attempts to use this class."""

    def __getattr__(self, _):
        raise ImproperlyConfigured(
            "Search client not configured. Start with settings.ENABLE_SEARCHIFY")