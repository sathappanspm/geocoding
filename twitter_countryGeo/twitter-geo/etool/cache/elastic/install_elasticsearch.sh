#!/bin/bash
# Setup basic Elasticsearch configuration, be sure to execute as 'ubuntu' user
BASEDIR=$(dirname $0)
ES_HOME="/mnt/embers"

sudo apt-get update
sudo apt-get --assume-yes install supervisor
sudo apt-get --assume-yes install openjdk-7-jre-headless

if [ ! -d $ES_HOME ]; then
    sudo mkdir $ES_HOME
fi

sudo chmod -R 777 $ES_HOME

wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.4.deb
sudo dpkg -i elasticsearch-1.4.4.deb
rm -f elasticsearch-1.4.4.deb

cd /usr/share/elasticsearch
sudo bin/plugin --install mobz/elasticsearch-head
sudo bin/plugin install elasticsearch/elasticsearch-cloud-aws/2.4.1

sudo mv $BASEDIR/conf/*.yml /etc/elasticsearch

cd $ES_HOME
sudo wget https://download.elasticsearch.org/kibana/kibana/kibana-4.0.1-linux-x64.tar.gz
sudo tar -xvzf kibana-4.0.1-linux-x64.tar.gz
sudo rm -f kibana-4.0.1-linux-x64.tar.gz
