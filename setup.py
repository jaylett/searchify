# Use setuptools if we can
try:
    from setuptools.core import setup
except ImportError:
    from distutils.core import setup

PACKAGE = 'django-searchify'
VERSION = '0.1'

setup(
    name=PACKAGE, version=VERSION,
    description="Search integration for Django with a focus on indexing",
    packages=[ 'searchify' ],
    license='MIT',
    author='James Aylett',
    author_email='james@tartarus.org',
    install_requires=[
        'Django>=1.3',
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Framework :: Django',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
    ],
)
