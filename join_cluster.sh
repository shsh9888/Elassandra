#first argument is node name (mandatory)
#second argument is VM instance IP(mandatory)

#third argument <ip of all seeds comma seperated>(mandatory)

eval sudo docker run --name $1 -d -e CASSANDRA_BROADCAST_ADDRESS=$2 -e "http.cors.enabled=true" -e "http.cors.allow-origin=*" -p 9042:9042 -p 7000:7000 -p 9200:9200 -p 9300:9300 -p 9160:9160 -p 7199:7199 -p 7001:7001 -e CASSANDRA_SEEDS=$3 strapdata/elassandra
