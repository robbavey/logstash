/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.logstash.common.io;


import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.DLQEntry;
import org.logstash.Event;
import org.logstash.Timestamp;
import org.logstash.ackedqueue.StringElement;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.logstash.common.io.RecordIOWriter.VERSION_SIZE;

public class DeadLetterQueueReaderTest {
    private Path dir;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static String segmentFileName(int i) {
        return String.format(DeadLetterQueueWriter.SEGMENT_FILE_PATTERN, i);
    }

    @Before
    public void setUp() throws Exception {
        dir = temporaryFolder.newFolder().toPath();
    }

    @Test
    public void testReadFromTwoSegments() throws Exception {
        RecordIOWriter writer = null;

        for (int i = 0; i < 5; i++) {
            Path segmentPath = dir.resolve(segmentFileName(i));
            writer = new RecordIOWriter(segmentPath);
            for (int j = 0; j < 10; j++) {
                writer.writeEvent((new StringElement("" + (i * 10 + j))).serialize());
            }
            if (i < 4) {
                writer.close();
            }
        }

        DeadLetterQueueReader manager = new DeadLetterQueueReader(dir);

        for (int i = 0; i < 50; i++) {
            String first = StringElement.deserialize(manager.pollEntryBytes()).toString();
            assertThat(first, equalTo(String.valueOf(i)));
        }

        assertThat(manager.pollEntryBytes(), is(nullValue()));
        assertThat(manager.pollEntryBytes(), is(nullValue()));
        assertThat(manager.pollEntryBytes(), is(nullValue()));
        assertThat(manager.pollEntryBytes(), is(nullValue()));

        for (int j = 50; j < 60; j++) {
            writer.writeEvent((new StringElement(String.valueOf(j))).serialize());
        }

        for (int i = 50; i < 60; i++) {
            String first = StringElement.deserialize(manager.pollEntryBytes()).toString();
            assertThat(first, equalTo(String.valueOf(i)));
        }

        writer.close();

        Path segmentPath = dir.resolve(segmentFileName(5));
        writer = new RecordIOWriter(segmentPath);

        for (int j = 0; j < 10; j++) {
            writer.writeEvent((new StringElement(String.valueOf(j))).serialize());
        }


        for (int i = 0; i < 10; i++) {
            byte[] read = manager.pollEntryBytes();
            while (read == null) {
                read = manager.pollEntryBytes();
            }
            String first = StringElement.deserialize(read).toString();
            assertThat(first, equalTo(String.valueOf(i)));
        }


        manager.close();
    }

    @Test
    public void testSeek() throws Exception {
        DeadLetterQueueWriter writeManager = new DeadLetterQueueWriter(dir, 10000000, 10000000);
        Event event = new Event(Collections.emptyMap());
        Timestamp target = null;
        long currentEpoch = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++){
            DLQEntry entry = new DLQEntry(event, "foo", "bar", String.valueOf(i), new Timestamp(currentEpoch++));
            writeManager.writeEntry(entry);
            if (i == 543) {
                target = entry.getEntryTime();
            }

        }
        writeManager.close();

        DeadLetterQueueReader readManager = new DeadLetterQueueReader(dir);
        readManager.seekToNextEvent(target);
        DLQEntry entry = readManager.pollEntry(100);
        assertThat(entry.getEntryTime().toIso8601(), equalTo(target.toIso8601()));
        assertThat(entry.getReason(), equalTo("543"));
    }

    @Test
    public void testSeekToStartOfRemovedLog() throws Exception {
        writeSegmentSizeEntries(3);
        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 0, 3, 1);
        startLog.toFile().delete();
        validateEntries(startLog, 1, 3, 1);
    }

    @Test
    public void testSeekToMiddleOfRemovedLog() throws Exception {
        writeSegmentSizeEntries(3);
        Path startLog = dir.resolve("1.log");
        startLog.toFile().delete();
        validateEntries(startLog, 1, 3, 32);
    }

    private void writeSegmentSizeEntries(int count) throws IOException {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int size = templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE;
        DeadLetterQueueWriter writeManager = null;
        try {
            writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
            for (int i = 0; i < count; i++) {
                writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i)));
            }
        } finally {
            writeManager.close();
        }
    }


    private void validateEntries(Path firstLog, int startEntry, int endEntry, int startPosition) throws IOException, InterruptedException {
        DeadLetterQueueReader readManager = null;
        try {
            readManager = new DeadLetterQueueReader(dir);
            readManager.setCurrentReaderAndPosition(firstLog, startPosition);
            for (int i = startEntry; i < endEntry; i++) {
                DLQEntry readEntry = readManager.pollEntry(100);
                assertThat(readEntry.getReason(), equalTo(String.valueOf(i)));
            }
        } finally {
            readManager.close();
        }
    }

    @Test
    public void testInvalidDirectory()  throws Exception {
        DeadLetterQueueReader reader = new DeadLetterQueueReader(dir);
        assertThat(reader.pollEntry(100), is(nullValue()));
    }
}
