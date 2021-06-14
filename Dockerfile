##
## Copyright (c) 2021 AusSRC.
##
## This file is part of SoFiAX
## (see https://github.com/AusSRC/SoFiAX).
##
## This program is free software: you can redistribute it and/or modify
## it under the terms of the GNU Lesser General Public License as published by
## the Free Software Foundation, either version 2.1 of the License, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU Lesser General Public License for more details.
##
## You should have received a copy of the GNU Lesser General Public License
## along with this program. If not, see <http://www.gnu.org/licenses/>.##
FROM python:3.8
WORKDIR /app

RUN git clone https://github.com/AusSRC/SoFiAX.git &&\
    cd SoFiAX &&\
    python3 setup.py install

ENTRYPOINT [ "sofiax" ]