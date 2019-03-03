package com.github.adamfisher.nifi.dns;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.xbill.DNS.DClass;

import java.util.Arrays;
import java.util.HashSet;

public class ZoneFileSchema extends SimpleRecordSchema {

    public static final String NAME_FIELD = "name";
    public static final String TTL_FIELD = "ttl";
    public static final String CLASS_FIELD = "class";
    public static final String TYPE_FIELD = "type";
    public static final String DATA_FIELD = "data";

    public ZoneFileSchema() {
        super(Arrays.asList(
                    new RecordField(NAME_FIELD, RecordFieldType.STRING.getDataType()),
                    new RecordField(TTL_FIELD, RecordFieldType.LONG.getDataType(), new HashSet<>(Arrays.asList("Time To Live", "time-to-live"))),
                    new RecordField(CLASS_FIELD, RecordFieldType.STRING.getDataType(), DClass.string(DClass.IN), new HashSet<>(Arrays.asList("Record Class", "record-class"))),
                    new RecordField(TYPE_FIELD, RecordFieldType.STRING.getDataType(), new HashSet<>(Arrays.asList("Record Type", "record-type"))),
                    new RecordField(DATA_FIELD, RecordFieldType.STRING.getDataType(), new HashSet<>(Arrays.asList("Record Data", "record-data")))
                ),
                SchemaIdentifier.builder()
                    .id(435783475224324L)
                    .name("ZoneFile")
                    .branch("RFC1035")
                    .version(1)
                    .build());
    }
}