from setuptools import setup
import sys

classifiers = ['Development Status :: 3 - Alpha',
               'Programming Language :: Python :: 2',
               'Programming Language :: Python :: 2.6',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: 3',
               'Programming Language :: Python :: 3.4',
               'Programming Language :: Python :: 3.5',
               'Programming Language :: Python :: Implementation :: CPython',
               'License :: OSI Approved :: BSD License']

install_requires = ['arrow>=0.7.0,<1',
                    'helper>=2.4.1',
                    'pika>=0.10.0',
                    'psutil>=3.3.0',
                    'queries>=1.8.1,<2',
                    'tornado>=4.2,<4.3']

extras_require = {'sentry': ['raven']}

if sys.version_info < (2, 7, 0):
    install_requires.append('backport_collections')

setup(name='mikkoo',
      version='0.4.5',
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
