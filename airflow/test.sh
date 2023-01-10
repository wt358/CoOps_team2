#!/bin/sh

TAG=$1
message=$2

docker images
docker build -t wcu5i9i6.kr.private-ncr.ntruss.com/cuda:$TAG -f gpu-Dockerfile .
docker push wcu5i9i6.kr.private-ncr.ntruss.com/cuda:$TAG

git add .
git commit -m i"$2"
git push origin main
