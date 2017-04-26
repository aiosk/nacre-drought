#!/bin/sh
docker-compose up --remove-orphans -d \
&& docker-compose logs -f
