from setuptools import setup
import re
import ast


_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('stan/aio/client.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))

setup(
    name='asyncio-nats-streaming',
    version=version,
    description='NATS Streaming client for Python Asyncio',
    long_description='Asyncio based Python client for NATS Streaming',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
        ],
    url='https://github.com/nats-io/stan.py',
    author='Waldemar Quevedo',
    author_email='wally@synadia.com',
    license='Apache 2 License',
    packages=['stan', 'stan.aio', 'stan.pb'],
    zip_safe=True,
    install_requires=['protobuf>=3.4','asyncio-nats-client>=0.7.0'],
)
