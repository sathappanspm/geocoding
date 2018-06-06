################################################################################
# Dockerfile definition of the Elasticsearch image for Mercury
################################################################################

FROM ubuntu:artful

RUN apt-get -y update
RUN apt-get -y install default-jdk
RUN add-apt-repository -y ppa:webupd8team/java && apt-get -y update
RUN apt-get -y install oracle-java8-installer && apt-get -y install git

WORKDIR /deploy

ADD ../../rss /deploy/

RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.5.0.tar.gz
RUN tar -xvf elasticsearch-5.5.0.tar.gz
ENV ES_DIR /deploy/elasticsearch-5.5.0
EXPOSE 9200 9300
ENTRYPOINT ["bin/elasticsearch"]
#

RUN cd /deploy/rss/src ; chmod 777 ./setup.sh ; ./setup.sh

WORKDIR $ES_DIR

