# nifi-zonefile-record-serialization-services
A NiFi custom controller service to parse the contents of an RFC 1035 compliant zone file.'

## Getting Started

This package exposes a **ZoneFileReader** controller service.

Build the NAR file using:

```
mvn install
```

Place the NAR file inside your NiFi installation's `lib` directory.

### Controller Properties

 - Include Name Field
 - Include TTL Field
 - Include Record Class Field
 - Include Record Type Field
 - Include Record Data Field