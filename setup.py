#
# Copyright (c) 2021 AusSRC.
#
# This file is part of SoFiAX
# (see https://github.com/AusSRC/SoFiAX).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 2.1 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.#

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
        "wheel",
        "extension-helpers",
        "aiofiles",
        "asyncpg",
        "xmltodict",
        "astropy"
    ],
    python_requires='>3.6',
)
