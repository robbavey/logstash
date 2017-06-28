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
import java.util.function.Function;

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
        Event event = new Event(Collections.emptyMap());
        long currentEpoch = System.currentTimeMillis();
        int TARGET_EVENT = 543;

        writeEntries(event, 0, 1000, currentEpoch);
        seekReadAndVerify(new Timestamp(currentEpoch + TARGET_EVENT),
                          String.valueOf(TARGET_EVENT));
    }


    @Test
    public void testSizeLogic() throws Exception {

    }

    @Test
    public void testDeleteSingleEventSegmentsMatchTime() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int segmentSize = 1;
        int size = segmentSize * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 10;
        long epoch = 1490659200000L;
        Timestamp deleteBefore = null;

        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == 4){
                deleteBefore = new Timestamp(epoch);
            }
            epoch += 1000;
        }
        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 10, 1);
        System.out.println("delting segments befgore" + deleteBefore);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 4, 10, 1);
    }

    @Test
    public void testDeleteTwoSegmentsMatchingTimeBeforeFirstSegment() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        long epoch = 1490659200000L;
        Timestamp deleteBefore = new Timestamp(epoch - 1);
        int segmentSize = 8;
        int size = segmentSize * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 10;

        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            epoch += 1000;
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 10, 1);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 1, 10, 1);
    }

    @Test
    public void testDeleteTwoSegmentsMatchingTimeStartOfFirstSegment() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        long epoch = 1490659200000L;
        int segmentSize = 8;
        int size = segmentSize * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 10;

        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        Timestamp deleteBefore = null;
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == 1){
                deleteBefore = new Timestamp(epoch);
            }
            epoch += 1000;
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 10, 1);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 1, 10, 1);
    }

    @Test
    public void testDeleteTwoSegmentsMatchingTimeInMiddleOfFirstSegment() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        long epoch = 1490659200000L;
        int segmentSize = 8;
        int size = segmentSize * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 10;

        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        Timestamp deleteBefore = null;
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == 4){
                deleteBefore = new Timestamp(epoch);
            }
            epoch += 1000;
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 10, 1);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 1, 10, 1);
    }

    @Test
    public void testDeleteTwoSegmentsMatchingTimeEndOfFirstSegment() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        long epoch = 1490659200000L;
        int segmentSize = 8;
        int size = segmentSize * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 10;

        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        Timestamp deleteBefore = null;
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == segmentSize){
                deleteBefore = new Timestamp(epoch);
            }
            epoch += 1000;
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 10, 1);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 1, 10, 1);
    }


    @Test
    public void testDeleteTwoSegmentsMatchingTimeAfterFirstButBeforeSecondSegment() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        long epoch = 1490659200000L;
        int segmentSize = 8;
        int size = segmentSize * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 16;

        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        Timestamp deleteBefore = null;
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == segmentSize){
                deleteBefore = new Timestamp(epoch + 1);
            }
            epoch += 1000;
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, count, 1);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 9, count, 1);
    }

    @Test
    public void testDeleteTwoSegmentsMatchingTimeStartOfSecondSegment() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        long epoch = 1490659200000L;
        int segmentSize = 8;
        int size = segmentSize * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 16;

        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        Timestamp deleteBefore = null;
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == 9){
                deleteBefore = new Timestamp(epoch);
            }
            epoch += 1000;
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, count, 1);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 9, count, 1);
    }

    @Test
    public void testDeleteTwoSegmentsMatchingTimeMiddleOfSecondSegment() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        long epoch = 1490659200000L;
        int segmentSize = 8;
        int size = segmentSize * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 16;

        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        Timestamp deleteBefore = null;
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == 12){
                deleteBefore = new Timestamp(epoch);
            }
            epoch += 1000;
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, count, 1);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 9, count, 1);
    }

    @Test
    public void testDeleteTwoSegmentsMatchingTimeEndOfSecondSegment() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        long epoch = 1490659200000L;
        int segmentSize = 8;
        int size = segmentSize * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 16;

        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        Timestamp deleteBefore = null;
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == 16){
                deleteBefore = new Timestamp(epoch);
            }
            epoch += 1000;
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, count, 1);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 9, count, 1);
    }

    @Test
    public void testDeleteTwoSegmentsNoMatchingTime() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        long epoch = 1490659200000L;
        int segmentSize = 8;
        int size = segmentSize * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 16;

        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));
            epoch += 1000;
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, count, 1);
        writeManager.deleteSegmentsOlderThan(Timestamp.now());
        validateEntries(startLog, 9, count, 1);
    }

    @Test
    public void testDeleteMultiEntrySegmentsMatchingTimeInSecondSegment() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int size = 8 * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 10;
        long epoch = 1490659200000L;
        Timestamp deleteBefore = null;
        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == 9){
                deleteBefore = new Timestamp(epoch);
            }
            epoch += 1000;
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 10, 1);
        System.out.println("delting segments befgore" + deleteBefore);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 9, 10, 1);
    }

    @Test
    public void testDeleteMultiEntrySegmentsMatchingTimePlusOneInSecondSegment() throws Exception {
//        writeSegmentSizeEntries(3);
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int size = 8 * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 10;
        long epoch = 1490659200000L;
        Timestamp deleteBefore = null;
        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == 9){
                deleteBefore = new Timestamp(epoch + 1);
            }
            epoch += 1000;
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 10, 1);
        System.out.println("delting segments befgore" + deleteBefore);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 9, 10, 1);
    }

    @Test
    public void testDeleteMultiEntrySegmentsMatchingTimeMinusOneInSecondSegment() throws Exception {
//        writeSegmentSizeEntries(3);
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int size = 8 * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 10;
        long epoch = 1490659200000L;
        Timestamp deleteBefore = null;
        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == 9){
                deleteBefore = new Timestamp(epoch - 1);
            }
            epoch += 1000;
        }
        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 10, 1);
        System.out.println("delting segments befgore" + deleteBefore);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 9, 10, 1);
    }

    @Test
    public void testDeleteNowMultiSegment() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int size = 8 * (templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE);
        int count = 10;
        long epoch = 1490659200000L;
        Timestamp deleteBefore = Timestamp.now();
        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));
            epoch += 1000;
        }
        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 10, 1);
        System.out.println("delting segments befgore" + deleteBefore);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 9, 10, 1);
    }

    @Test
    public void testDeleteNowTinySegment() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int size = templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE;
        int count = 10;
        long epoch = 1490659200000L;
        Timestamp deleteBefore = Timestamp.now();
        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));
            epoch += 1000;
        }
        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 10, 1);
        System.out.println("delting segments befgore" + deleteBefore);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 10, 10, 1);
    }

    @Test
    public void testDeleteSegmentsPlusOne() throws Exception {
//        writeSegmentSizeEntries(3);
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int size = templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE;
        int count = 10;
        long epoch = 1490659200000L;
        Timestamp deleteBefore = null;
        DeadLetterQueueWriter writeManager = writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        try {
//            writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
            for (int i = 1; i <= count; i++) {
                writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

                if (i == 4){
                    deleteBefore = new Timestamp(epoch+1);
                }
                epoch += 1000;
            }
        } finally {
//            writeManager.close();
        }

        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 10, 1);
//        writeManager.deleteSegmentsBefore(deleteBefore);
        System.out.println("delting segments befgore" + deleteBefore);
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 5, 10, 1);
    }

    @Test
    public void testDeleteSegmentsMinusOne() throws Exception {
//        writeSegmentSizeEntries(3);
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int size = templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE;
        int count = 10;
        long epoch = 1490659200000L;
        Timestamp deleteBefore = null;
        DeadLetterQueueWriter writeManager = null;
        try {
            writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
            for (int i = 1; i <= 10; i++) {
                writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

                if (i == 4){
                    deleteBefore = new Timestamp(epoch-1);
                }
                epoch += 1000;
            }
        } finally {
            writeManager.close();
        }

        Path startLog = dir.resolve("1.log");
        System.out.println("Deleting entries before " + deleteBefore.toIso8601());
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 4, 10, 1);
    }

    @Test
    public void testSegmentsBeforeTPlus1() throws Exception {
//        writeSegmentSizeEntries(3);
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int size = templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE;
        int count = 10;
        long epoch = 1490659200000L;
        Timestamp deleteBefore = null;
        DeadLetterQueueWriter writeManager = null;
        try {
            writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
            for (int i = 1; i <= 10; i++) {
                writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

                if (i == 4){
                    deleteBefore = new Timestamp(epoch-1);
                }
                epoch += 1000;
            }
        } finally {
            writeManager.close();
        }

        Path startLog = dir.resolve("1.log");

        validateEntries(startLog, 1, 10, 1);
        System.out.println("Validated first 10");
        writeManager.deleteSegmentsOlderThan(deleteBefore);
        validateEntries(startLog, 4, 10, 1);
    }

    @Test
    public void testDeleteBiggerThan() throws Exception {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int size = templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE;
        writeSegmentSizeEntries(30);
        Path startLog = dir.resolve("1.log");
        DeadLetterQueueWriter writeManager = null;
        writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
        System.out.println(writeManager.getCurrentQueueSize());

        validateEntries(startLog, 1, 3, 1);
        System.out.println("Validate the first 3");

        writeManager.deleteSegmentsUntilSmallerThan(320);
        validateEntries(startLog, 29, 3, 1);
    }


    @Test
    public void testSeekToStartOfRemovedLog() throws Exception {
        writeSegmentSizeEntries(3);
        Path startLog = dir.resolve("1.log");
        validateEntries(startLog, 1, 3, 1);
        startLog.toFile().delete();
        validateEntries(startLog, 2, 3, 1);
    }

    @Test
    public void testSeekToMiddleOfRemovedLog() throws Exception {
        writeSegmentSizeEntries(3);
        Path startLog = dir.resolve("1.log");
        startLog.toFile().delete();
        validateEntries(startLog, 2, 3, 32);
    }

    private void writeSegmentSizeEntries(int count) throws IOException {
        Event event = new Event(Collections.emptyMap());
        DLQEntry templateEntry = new DLQEntry(event, "1", "1", String.valueOf(0));
        int size = templateEntry.serialize().length + RecordIOWriter.RECORD_HEADER_SIZE + VERSION_SIZE;
        DeadLetterQueueWriter writeManager = null;
        try {
            writeManager = new DeadLetterQueueWriter(dir, size, 10000000);
            for (int i = 1; i <= count; i++) {
                writeManager.writeEntry(new DLQEntry(event, String.valueOf(i), String.valueOf(i), String.valueOf(i)));
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
            for (int i = startEntry; i <= endEntry; i++) {
                DLQEntry readEntry = readManager.pollEntry(100);
                if (readEntry != null)
                System.out.println("Time of entry is " + readEntry.getEntryTime().toIso8601() + ": entry is " + readEntry.getReason());
                assertThat(readEntry.getReason(), equalTo(String.valueOf(i)));
            }
        } finally {
            readManager.close();
        }
    }

    @Test
    public void testWriteStopSmallWriteSeekByTimestamp() throws Exception {
        int FIRST_WRITE_EVENT_COUNT = 100;
        int SECOND_WRITE_EVENT_COUNT = 100;
        int OFFSET = 200;

        Event event = new Event(Collections.emptyMap());
        long startTime = System.currentTimeMillis();

        writeEntries(event, 0, FIRST_WRITE_EVENT_COUNT, startTime);
        writeEntries(event, OFFSET, SECOND_WRITE_EVENT_COUNT, startTime + 1_000);

        seekReadAndVerify(new Timestamp(startTime + FIRST_WRITE_EVENT_COUNT),
                          String.valueOf(FIRST_WRITE_EVENT_COUNT));
    }
    private Timestamp writeEvents(final Event event, long epoch, final int segmentSize, final int count, final Function<Long, Timestamp> breakingTimestamp, final DeadLetterQueueWriter writeManager) throws IOException {
        Timestamp deleteBefore = null;
        for (int i = 1; i <= count; i++) {
            writeManager.writeEntry(new DLQEntry(event, "1", "1", String.valueOf(i), new Timestamp(epoch)));

            if (i == segmentSize){
                deleteBefore = breakingTimestamp.apply(epoch);
            }
            epoch += 1000;
        }
        return deleteBefore;
    }



    @Test
    public void testWriteStopBigWriteSeekByTimestamp() throws Exception {
        int FIRST_WRITE_EVENT_COUNT = 100;
        int SECOND_WRITE_EVENT_COUNT = 2000;
        int OFFSET = 200;

        Event event = new Event(Collections.emptyMap());
        long startTime = System.currentTimeMillis();

        writeEntries(event, 0, FIRST_WRITE_EVENT_COUNT, startTime);
        writeEntries(event, OFFSET, SECOND_WRITE_EVENT_COUNT, startTime + 1_000);

        seekReadAndVerify(new Timestamp(startTime + FIRST_WRITE_EVENT_COUNT),
                          String.valueOf(FIRST_WRITE_EVENT_COUNT));
    }


    private void seekReadAndVerify(final Timestamp seekTarget, final String expectedValue) throws Exception {
        try(DeadLetterQueueReader readManager = new DeadLetterQueueReader(dir)) {
            readManager.seekToNextEvent(new Timestamp(seekTarget));
            DLQEntry readEntry = readManager.pollEntry(100);
            assertThat(readEntry.getReason(), equalTo(expectedValue));
            assertThat(readEntry.getEntryTime().toIso8601(), equalTo(seekTarget.toIso8601()));
        }
    }

    private void writeEntries(final Event event, int offset, final int numberOfEvents, long startTime) throws IOException {
        try(DeadLetterQueueWriter writeManager = new DeadLetterQueueWriter(dir, 10000000, 10000000)) {
            for (int i = offset; i <= offset + numberOfEvents; i++) {
                DLQEntry entry = new DLQEntry(event, "foo", "bar", String.valueOf(i), new Timestamp(startTime++));
                writeManager.writeEntry(entry);
            }
        }
    }

}
