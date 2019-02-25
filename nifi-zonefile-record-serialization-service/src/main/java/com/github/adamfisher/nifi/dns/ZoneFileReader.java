/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.adamfisher.nifi.dns;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.*;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

@Tags({"bind", "zone", "zone file", "parse", "record", "row", "reader", "delimited", "separated", "values"})
@CapabilityDescription("Parses RFC 1035 compliant zone files.")
public class ZoneFileReader extends AbstractControllerService implements RecordReaderFactory {

    public static final PropertyDescriptor INCLUDE_NAME_FIELD = new PropertyDescriptor
            .Builder().name("name-field")
            .displayName("Include Name Field")
            .description("Includes the name field of a zone file record in the output FlowFile.")
            .allowableValues("true", "false")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_TTL_FIELD = new PropertyDescriptor
            .Builder().name("ttl-field")
            .displayName("Include TTL Field")
            .description("Includes the TTL field of a zone file record in the output FlowFile.")
            .allowableValues("true", "false")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_RECORD_CLASS_FIELD = new PropertyDescriptor
            .Builder().name("record-class-field")
            .displayName("Include Record Class Field")
            .description("Includes the record class field of a zone file record in the output FlowFile.")
            .allowableValues("true", "false")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_RECORD_TYPE_FIELD = new PropertyDescriptor
            .Builder().name("record-type-field")
            .displayName("Include Record Type Field")
            .description("Includes the record type field of a zone file record in the output FlowFile.")
            .allowableValues("true", "false")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_RECORD_DATA_FIELD = new PropertyDescriptor
            .Builder().name("record-data-field")
            .displayName("Include Record Data Field")
            .description("Includes the record data field of a zone file record in the output FlowFile.")
            .allowableValues("true", "false")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private volatile boolean includeNameField;
    private volatile boolean includeTtlField;
    private volatile boolean includeRecordClassField;
    private volatile boolean includeRecordTypeField;
    private volatile boolean includeRecordDataField;
    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(INCLUDE_NAME_FIELD);
        props.add(INCLUDE_TTL_FIELD);
        props.add(INCLUDE_RECORD_CLASS_FIELD);
        props.add(INCLUDE_RECORD_TYPE_FIELD);
        props.add(INCLUDE_RECORD_DATA_FIELD);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void cacheIncludedFieldNames(final ConfigurationContext context) {
        this.includeNameField = context.getProperty(INCLUDE_NAME_FIELD).asBoolean();
        this.includeTtlField = context.getProperty(INCLUDE_TTL_FIELD).asBoolean();
        this.includeRecordClassField = context.getProperty(INCLUDE_RECORD_CLASS_FIELD).asBoolean();
        this.includeRecordTypeField = context.getProperty(INCLUDE_RECORD_TYPE_FIELD).asBoolean();
        this.includeRecordDataField = context.getProperty(INCLUDE_RECORD_DATA_FIELD).asBoolean();
    }

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        return new ZoneFileRecordReader(
                in,
                includeNameField,
                includeTtlField,
                includeRecordClassField,
                includeRecordTypeField,
                includeRecordDataField);
    }
}
