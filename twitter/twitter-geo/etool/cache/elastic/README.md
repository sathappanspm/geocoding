The scripts in this directory all relate to querying and caching data into 
[Elasticsearch](https://www.elastic.co/products/elasticsearch)

# Elasticsearch Configuration

The scripts are defaulted to set the [Elasticsearch index](https://www.elastic.co/blog/what-is-an-elasticsearch-index/) 
name to the name of the cluster in which the script is executing and the [type](http://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html) 
name is set to the EMBERS queue names listed in embers.conf.
 
General configuration can be found in [general.py](general.py) and [conf/elasticsearch.yml](conf/elasticsearch.yml). 
Each new Elasticsearch type added will have the default mapping found in [conf/mapping/default_mapping.json](conf/mapping/default_mapping.json).
Here you can add any special configuration for any of the fields you like by following [these instructions](http://www.elastic.co/guide/en/elasticsearch/guide/current/custom-dynamic-mapping.html)

# Development Machine Configuration

You will need to have an environment variable name CLUSTERNAME set to the cluster you wish to query to be able to 
execute some of the functions and scripts. The EMBERS deployment scripts already handles this for each cluster 
appropriately, but if you are testing from a machine outside of the cluster, the value for CLUSTERNAME should be 'test' 
or 'production' so that you don't have to change it for every new cluster created.

# Python Libraries

* cache.py - Contains common functions for caching data into Elasticsearch from several of the EMBERS data sources (queues, 
            [Simple DB](http://aws.amazon.com/simpledb/), [S3](http://aws.amazon.com/s3/))
* general.py - Contains common Elasticsearch functions for retrieving connections, querying and parsing results.  
* index_setup.py - Contains functions to set up new Elasticsearch indices and type mapping
* twitter.py - Contains functions to query EMBERS Twitter data contained in Elasticsearch. Some of the functions may be 
            set up for specific EMBERS purposes (i.e. Dynamic Query Expansion) and should be understood before altering. 
* warnings_api.py - Contains functions to query EMBERS Warnings data contained in Elasticsearch. 

# Setup Scripts

* install_elaticsearch.sh - Installs and configures Elasticsearch onto any *nix machine that uses [Debian package manager](https://wiki.debian.org/apt-get) ('apt-get')
* install_logstash.sh - Installs [logstash](http://logstash.net/). Logstash isn't currently being utilized 