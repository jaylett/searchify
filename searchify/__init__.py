# Note that much of the time, something that's a singleton (eg: a string) can also be an iterable. Where this isn't the case, it probably should be.
# In your search field names, don't start with an underscore ('_') as that's reserved.
# We STRONGLY recommend explicitly declaring your search field names, as it makes the resultant search system more useful to users. In some cases you don't need it, but that's rare.
# Note that strictly you can have a callable as a django_field directly. In this case, it will be called with a parameter of None to generate the search field name (well, part of it - it only needs to be unique to the class). But don't do this, it's ugly.
# When auto-generating, we use '.' to separate bits of things where possible, and '__' where we require \w only.

# FIXME: document the query() method added to managers
# FIXME: make query() do pagination properly, on top of anything Flax chooses to offer us (currently Flax gives us nothing)
# FIXME: detect and resolve circular cascades

# TODO: make it possible to index an individual model to more than one database. (Probably multiple explicit indexers.)
# TODO: reverse cascades, so you can put searchable stuff into your Profile model, but have it index stuff from the User. (Also just easier in general, although I can't see how to make it as powerful as normal cascades.)
# TODO: if you change the django_fields, searchify should "want" to reindex, with suitable options to tell it not to; perhaps a hash of the config (and allow it to be set explicitly for people who want to manage this themselves)

from index import register_indexer, autodiscover, reindex, Indexer, get_searcher
