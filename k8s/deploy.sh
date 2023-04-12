#!/usr/bin/env bash

bold=$(tput bold)
normal=$(tput sgr0)

IS_DOCKER_DESKTOP=$(kubectl get nodes | grep "docker-desktop"| wc -l)

if [[ "$IS_DOCKER_DESKTOP" -eq "1" ]]
then
    kubectl apply -f docker-desktop/docker-standard-storage-class.yaml
#    kubectl apply -f docker-desktop/knife.yaml
else
    echo "not on docker desktop, standard storage class exists, skipping."
fi

docker image tag pichler-hugo-page ghcr.io/larapichler/pichler-hugo-page:latest
docker push ghcr.io/larapichler/pichler-hugo-page:latest
docker image tag hugo-backend-pichler ghcr.io/larapichler/hugo-backend-pichler:latest
docker push ghcr.io/larapichler/hugo-backend-pichler:latest
kubectl delete -f appsrv.yaml
kubectl delete -f nginx.yaml
kubectl apply -f namespace.yaml
kubectl apply -f appsrv.yaml
kubectl apply -f nginx.yaml
kubectl apply -f ingress.yaml

echo "----------"
echo "DO NOT FORGET: make the ${bold}docker image public${normal} on ghcr.io"
echo "----------"
