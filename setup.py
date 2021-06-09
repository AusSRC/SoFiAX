from setuptools import setup

setup(
    name='SoFiAX',
    version='0.1.0',
    author='',
    author_email='',
    packages=['src.utils', 'src'],
    url='',
    license='LICENSE.txt',
    description='SoFiAX',
    long_description=open('README.md').read(),
    install_requires=[
        "aiofiles",
        "asyncpg",
        "xmltodict",
    ],
    python_requires='>3.6',
    entry_points={
        'console_scripts': ['sofiax=src.main:main'],
    }
)
