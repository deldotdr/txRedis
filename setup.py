#!/usr/bin/env python
sdict = {
    'name' : 'txredis',
    'version' : '2.2',
	'packages' : ['txredis'],
    'description' : 'Python/Twisted client for Redis key-value store',
    'author' : 'Dorian Raymer',
    'author_email' : 'deldotdr@gmail.com',
    'maintainer' : 'Reza Lotun',
    'maintainer_email' : 'rlotun@gmail.com',
    'keywords': ['Redis', 'key-value store', 'Twisted'],
    'classifiers' : [
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Framework :: Twisted',
        ],
}

from setuptools import setup
setup(**sdict)
