from setuptools import setup
from stan.aio.client import __version__

setup(
    name='asyncio-nats-streaming',
    version=__version__,
    description='NATS Streaming client for Python Asyncio',
    long_description='Asyncio based Python client for NATS Streaming',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6'
        ],
    url='https://github.com/nats-io/asyncio-nats-streaming',
    author='Waldemar Quevedo',
    author_email='wally@apcera.com',
    license='MIT License',
    packages=['stan', 'stan.aio', 'stan.pb'],
    zip_safe=True,
    install_requires=['protobuf>=3.4','asyncio-nats-client>=0.5.0'],
)
