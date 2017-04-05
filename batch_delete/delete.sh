#!/bin/bash

for line in `cat $1`
do
  echo '1.0.0/'$line'/*'
  s3rm -f --profile ltjeg_aws_s3_tiles --bucket-name akiai4jxkwjqv5tgsaoq-wmts --prefix '1.0.0/'$line'/*'
done

