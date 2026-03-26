#!/bin/bash

while IFS= read -r url;
do
    wget -P $JARS_DIR $url
done < "$1"