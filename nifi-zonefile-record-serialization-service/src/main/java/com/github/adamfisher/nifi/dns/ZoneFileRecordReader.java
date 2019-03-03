package com.github.adamfisher.nifi.dns;

import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.*;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Master;
import org.xbill.DNS.Type;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class ZoneFileRecordReader implements RecordReader {

    protected final Master reader;
    protected final RecordSchema schema;

    public ZoneFileRecordReader(final InputStream in) {
        this.reader = new Master(in);
        this.schema = new ZoneFileSchema();
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException {
        final org.xbill.DNS.Record zoneFileRecord = reader.nextRecord();

        if(zoneFileRecord != null) {
            final Map<String, Object> fieldValues = new LinkedHashMap<>(5);
            fieldValues.put(ZoneFileSchema.NAME_FIELD, zoneFileRecord.getName());
            fieldValues.put(ZoneFileSchema.TTL_FIELD, zoneFileRecord.getTTL());
            fieldValues.put(ZoneFileSchema.CLASS_FIELD, DClass.string(zoneFileRecord.getDClass()));
            fieldValues.put(ZoneFileSchema.TYPE_FIELD, Type.string(zoneFileRecord.getType()));
            fieldValues.put(ZoneFileSchema.DATA_FIELD, zoneFileRecord.rdataToString());
            return new MapRecord(getSchema(), fieldValues);
        }

        return null;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public void close() throws IOException {
    }
}
