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

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.Record;
import org.junit.Test;
import org.xbill.DNS.Type;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class TestZoneFileRecordReader {

    @Test
     public void testReadingZoneFile() {
        final InputStream in = getClass().getClassLoader().getResourceAsStream("com.zone");
        final ZoneFileRecordReader reader = new ZoneFileRecordReader(in);
        final Supplier<Stream<Record>> records = streamZoneFileContents(reader);

        assertThat(records.get().filter(r -> r.getAsString(ZoneFileSchema.TYPE_FIELD).equals(Type.string(Type.NS))).count(), equalTo(21L));

        List<Record> kitchenFloorTileNameServerRecords = records.get().filter(r ->
            r.getAsString(ZoneFileSchema.TYPE_FIELD).equals(Type.string(Type.NS))
            && r.getAsString(ZoneFileSchema.NAME_FIELD).equals("KITCHENFLOORTILE.COM."))
                .collect(Collectors.toList());

        assertThat(kitchenFloorTileNameServerRecords.size(), equalTo(2));
        assertThat(kitchenFloorTileNameServerRecords.stream().map(r -> r.getAsString(ZoneFileSchema.DATA_FIELD)).collect(Collectors.toList()),
                hasItems("NS1.UNIREGISTRYMARKET.LINK.", "NS2.UNIREGISTRYMARKET.LINK."));
    }

    private Supplier<Stream<Record>> streamZoneFileContents(ZoneFileRecordReader reader) {
        List<Record> list = new ArrayList<>();
        Record record;
        try {
            while((record = reader.nextRecord()) != null) {
                list.add(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MalformedRecordException e) {
            e.printStackTrace();
        }
        return () -> list.stream();
    }
}
