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

FROM python:3.10.8-slim

RUN apt update && apt install -y wcslib-dev gcc git procps
RUN mkdir -p /app
RUN mkdir -p /input
RUN mkdir -p /output

WORKDIR /app

RUN git clone https://gitlab.com/SoFiA-Admin/SoFiA-2
WORKDIR ./SoFiA-2 
RUN ./compile.sh -fopenmp \
    && chmod +x /app/SoFiA-2/sofia \
    && ln -s /app/SoFiA-2/sofia /usr/bin/sofia

WORKDIR /app

COPY . /app/sofiax
WORKDIR /app/sofiax
RUN pip install --upgrade pip && pip install -r requirements.txt && python3 setup.py install

WORKDIR /app/sofiax
