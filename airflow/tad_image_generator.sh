#!/bin/sh

TAG=$1
message=$2

docker images
docker build -t wcu5i9i6.kr.private-ncr.ntruss.com/tad:$TAG -f tad-Dockerfile .
docker push wcu5i9i6.kr.private-ncr.ntruss.com/tad:$TAG
docker rmi wcu5i9i6.kr.private-ncr.ntruss.com/tad:$TAG

git add .
git commit -m "$2"
git push origin main
