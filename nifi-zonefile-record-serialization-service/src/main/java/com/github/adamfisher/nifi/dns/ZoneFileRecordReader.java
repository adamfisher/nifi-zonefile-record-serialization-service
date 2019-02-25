package com.github.adamfisher.nifi.dns;

import avro.shaded.com.google.common.base.Throwables;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Master;
import org.xbill.DNS.NSRecord;
import org.xbill.DNS.Type;
import org.xbill.DNS.spi.DNSJavaNameService;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class ZoneFileRecordReader implements RecordReader {

    protected final Master reader;
    protected final RecordSchema schema;
    private final boolean includeNameField;
    private final boolean includeTtlField;
    private final boolean includeRecordClassField;
    private final boolean includeRecordTypeField;
    private final boolean includeRecordDataField;

    public ZoneFileRecordReader(final InputStream in, boolean includeNameField, boolean includeTtlField,
                                boolean includeRecordClassField, boolean includeRecordTypeField, boolean includeRecordDataField) {
        this.reader = new Master(in);
        this.schema = new ZoneFileSchema();
        this.includeNameField = includeNameField;
        this.includeTtlField = includeTtlField;
        this.includeRecordClassField = includeRecordClassField;
        this.includeRecordTypeField = includeRecordTypeField;
        this.includeRecordDataField = includeRecordDataField;
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws MalformedRecordException {
        try {
            final org.xbill.DNS.Record zoneFileRecord = reader.nextRecord();

            if(zoneFileRecord != null) {
                final Map<String, Object> fieldValues = new LinkedHashMap<>(5);

                if (includeNameField)
                    fieldValues.put(ZoneFileSchema.NAME_FIELD, zoneFileRecord.getName());
                if (includeTtlField)
                    fieldValues.put(ZoneFileSchema.TTL_FIELD, zoneFileRecord.getTTL());
                if (includeRecordClassField)
                    fieldValues.put(ZoneFileSchema.CLASS_FIELD, DClass.string(zoneFileRecord.getDClass()));
                if (includeRecordTypeField)
                    fieldValues.put(ZoneFileSchema.TYPE_FIELD, Type.string(zoneFileRecord.getType()));
                if (includeRecordDataField)
                    fieldValues.put(ZoneFileSchema.DATA_FIELD, zoneFileRecord.rdataToString());

                return new MapRecord(getSchema(), fieldValues);
            }

        } catch (Exception e) {
            throw new MalformedRecordException("Error while getting next record.\nRoot cause: " +  Throwables.getRootCause(e), e);
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
