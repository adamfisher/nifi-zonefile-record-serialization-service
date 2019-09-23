# nifi-zonefile-record-serialization-services
[![Build Status](https://dev.azure.com/adamfisher/public/_apis/build/status/nifi-zonefile-record-serialization-services?branchName=master)](https://dev.azure.com/adamfisher/public/_build/latest?definitionId=1&branchName=master)

A NiFi custom controller service to parse the contents of an RFC 1035 compliant zone file.

## Getting Started

This package exposes a **ZoneFileReader** controller service.

Build the NAR file using:

```
mvn clean install
```

Place the NAR file inside your NiFi installation's `lib` directory.


### AVRO Schema Format
By default, the reader attaches the following schema to the outgoing record set:

```
{
   "type" : "record",
   "name" : "ZoneFile",
   "fields" : [
    { "name" : "name" , "type" : "string"},
	  { "name" : "ttl" , "type" : "long", "aliases" : ["Time To Live", "time-to-live"]},
	  { "name" : "class" , "type" : "string", "aliases" : ["Record Class", "record-class"]},
	  { "name" : "type" , "type" : "string", "aliases" : ["record-type"]},
	  { "name" : "data" , "type" : "string", "aliases" : ["record-data"]}
   ]
}
```
