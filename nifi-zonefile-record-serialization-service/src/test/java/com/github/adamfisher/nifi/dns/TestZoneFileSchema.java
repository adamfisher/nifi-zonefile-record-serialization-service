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

import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestZoneFileSchema {

    @Test
    public void testSchemaIdentifier() throws SchemaNotFoundException {
        final ZoneFileSchema schema = new ZoneFileSchema();

        assertEquals("Schema should contain five fields", 5, schema.getFieldCount());
        assertEquals("Schema identifier must be a specific value", 435783475224324L, schema.getIdentifier().getIdentifier().getAsLong());
        assertEquals("Schema name must be a specific value", "ZoneFile", schema.getIdentifier().getName().get());
        assertEquals("Schema branch must be a specific value", "RFC1035", schema.getIdentifier().getBranch().get());
        assertEquals("Schema version must be a specific value", 1, schema.getIdentifier().getVersion().getAsInt());
    }

    @Test
    public void allFieldsPresent() {
        final ZoneFileSchema schema = new ZoneFileSchema();
        assertFieldIsPresent(schema, ZoneFileSchema.NAME_FIELD);
        assertFieldIsPresent(schema, ZoneFileSchema.TTL_FIELD);
        assertFieldIsPresent(schema, ZoneFileSchema.CLASS_FIELD);
        assertFieldIsPresent(schema, ZoneFileSchema.TYPE_FIELD);
        assertFieldIsPresent(schema, ZoneFileSchema.DATA_FIELD);
    }

    private void assertFieldIsPresent(ZoneFileSchema schema, String fieldName) {
        assertTrue(fieldName + " field should be present", schema.getField(fieldName).isPresent());
    }
}
