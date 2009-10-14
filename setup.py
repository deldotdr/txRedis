#!/usr/bin/env python
sdict = {
    'name' : 'txredis',
    'version' : '0.2.0',
	'packages' : ['txredis'],
    'description' : 'Python/Twisted client for Redis key-value store',
    'url': 'http://amoeba.ucsd.edu/packages/txredis/',
    'download_url' : 'http://amoeba.ucsd.edu/packages/txredis/txredis-0.2.0.tar.gz',
    'author' : 'Dorian Raymer',
    'author_email' : 'deldotdr@gmail.com',
    'maintainer' : 'Dorian Raymer',
    'maintainer_email' : 'deldotdr@gmail.com',
    'keywords': ['Redis', 'key-value store', 'Twisted'],
    'classifiers' : [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python'],
}

from distutils.core import setup
setup(**sdict)
