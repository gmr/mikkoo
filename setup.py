from setuptools import setup

from mikkoo import __version__

classifiers = ['Development Status :: 5 - Production/Stable',
               'Programming Language :: Python :: 2',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: 3',
               'Programming Language :: Python :: 3.4',
               'Programming Language :: Python :: 3.5',
               'Programming Language :: Python :: Implementation :: CPython',
               'Programming Language :: Python :: Implementation :: PyPy',
               'License :: OSI Approved :: BSD License']

install_requires = ['arrow>=0.7.0,<1',
                    'helper>=2.4.1',
                    'pika>=0.10.0',
                    'psutil>=3.3.0',
                    'queries>=1.8.1,<2',
                    'simpleflake==0.1.5',
                    'tornado>=4.2,<4.3']

extras_require = {'sentry': ['raven']}

setup(name='mikkoo',
      version=__version__,
      description='Mikkoo is a PgQ to RabbitMQ Relay',
      long_description=open('README.rst').read(),
      classifiers=classifiers,
      keywords='amqp rabbitmq postgresql',
      author='Gavin M. Roy',
      author_email='gavinmroy@gmail.com',
      url='https://github.com/gmr/mikkoo',
      license='BSD',
      packages=['mikkoo'],
      package_data={'': ['LICENSE', 'README.rst']},
      include_package_data=True,
      install_requires=install_requires,
      tests_require=['mock', 'nose', 'unittest2'],
      entry_points=dict(console_scripts=['mikkoo=mikkoo.controller:main']),
      zip_safe=True)
