#!/bin/sh
FILE="go1.8.1.linux-armv6l.tar.gz"

if [ ! -f "./scraper/$FILE" ]; then
  curl -Lk -o "./scraper/$FILE" "https://storage.googleapis.com/golang/$FILE"
fi


docker-compose build
