#!/bin/bash

set -e
echo "Instalando docker"
sudo apt-get update

sudo apt-get install docker.io -y

sudo systemctl start docker
sudo systemctl enable docker


echo "Instalando Astro CLI"
curl -sSL install.astronomer.io | sudo bash -s

if [ -d "lakehouse-spain-mobility" ]; then
    cd lakehouse-spain-mobility
    sudo astro dev start
else
    echo "Error: El directorio 'lakehouse-spain-mobility' no existe."
    exit 1
fi