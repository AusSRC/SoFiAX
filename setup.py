from setuptools import setup

setup(
    name='SoFiAX',
    version='0.1.0',
    author='',
    author_email='',
    packages=['sofiax'],
    url='',
    license='LICENSE.txt',
    description='SoFiAX',
    long_description=open('README.md').read(),
    install_requires=[
        "aiofiles",
        "asyncpg",
        "xmltodict",
        "astropy"
    ],
    python_requires='>3.6',
    entry_points={
        'console_scripts': ['sofiax=sofiax.run:main'],
    }
)
