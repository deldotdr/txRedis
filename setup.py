#!/usr/bin/env python
sdict = {
    'name' : 'txredis',
    'version' : '0.1.1',
	'packages' : ['txredis'],
    'description' : 'Python/Twisted client for Redis key-value store',
    'url': 'http://ooici.net/packages/txredis/',
    'download_url' : 'http://ooici.net/packages/txredis/txredis-0.1.1.tar.gz',
    'author' : 'Dorian Raymer',
    'author_email' : 'deldotdr@gmail.com',
    'maintainer' : 'Dorian Raymer',
    'maintainer_email' : 'deldotdr@gmail.com',
    'keywords': ['Redis', 'key-value store', 'Twisted'],
    'classifiers' : [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Framework :: Twisted',
        ],
}

from distutils.core import setup
setup(**sdict)
