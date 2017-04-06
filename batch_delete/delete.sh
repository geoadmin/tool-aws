#!/bin/bash
set -e
set -u
set -o pipefail


for line in $(cat $1)
do
  prefix='1.0.0/'$line'/*'
  size=${#prefix}
  echo $prefix' ('$size')'
  if [ "$size" -gt 17 ]; then
    s3rm -f --profile ltjeg_aws_s3_tiles --bucket-name akiai4jxkwjqv5tgsaoq-wmts --prefix $prefix
  else
    echo 'ignored because prefix is too small '$prefix
  fi
done

