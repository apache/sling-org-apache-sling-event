/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.event.impl.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.sling.event.jobs.Job;
import org.junit.Test;

public class JobsImplTest {

    @Test public void testSorting() {
        final Calendar now = Calendar.getInstance();
        final Map<String, Object> properties = new HashMap<>();
        properties.put(Job.PROPERTY_JOB_CREATED, now);

        final JobImpl job1 = new JobImpl("test", "hello_1", properties);
        final JobImpl job2 = new JobImpl("test", "hello_2", properties);
        final JobImpl job3 = new JobImpl("test", "hello_4", properties);
        final JobImpl job4 = new JobImpl("test", "hello_30", properties);
        final JobImpl job5 = new JobImpl("test", "hello_50", properties);

        final List<JobImpl> list = new ArrayList<JobImpl>();
        list.add(job5);
        list.add(job2);
        list.add(job1);
        list.add(job4);
        list.add(job3);

        Collections.sort(list);

        assertEquals(job1, list.get(0));
        assertEquals(job2, list.get(1));
        assertEquals(job3, list.get(2));
        assertEquals(job4, list.get(3));
        assertEquals(job5, list.get(4));

    }

    @Test
    public void testProgressLogCount() {
        final Map<String, Object> properties = new HashMap<>();

        final JobImpl job = new JobImpl("test", "hello_1", properties);

        assertNull(job.getProperty(Job.PROPERTY_JOB_PROGRESS_LOG));
        assertNull(job.getProgressLog());

        for (int i = 0; i < 20; i++) {
            job.log(10, "message_" + i);
        }

        final String[] progressLog = job.getProgressLog();
        assertEquals(10, progressLog.length);
        for (int i = 0; i < 9; i++) {
            assertEquals("message_1" + i, progressLog[i]);
        }
        assertEquals("message_19" + JobImpl.TRUNCATED_LOG, progressLog[9]);
    }

    @Test
    public void testProgressLogCountWithZeroCount() {
        final Map<String, Object> properties = new HashMap<>();

        final JobImpl job = new JobImpl("test", "hello_1", properties);

        for (int i = 0; i < 2; i++) {
            job.log(0, "message_" + i);
        }

        assertNull(job.getProgressLog());
    }

    @Test
    public void testProgressLogCountWithNegativeCount() {
        final Map<String, Object> properties = new HashMap<>();

        final JobImpl job = new JobImpl("test", "hello_1", properties);

        for (int i = 0; i < 2; i++) {
            job.log(-2, "message_" + i);
        }

        assertNull(job.getProgressLog());
    }

    @Test
    public void testProgressLogCountWithInfiniteCount() {
        final Map<String, Object> properties = new HashMap<>();

        final JobImpl job = new JobImpl("test", "hello_1", properties);

        for (int i = 0; i < 20; i++) {
            job.log(Integer.MAX_VALUE, "message_" + i);
        }

        final String[] progressLog = job.getProgressLog();
        assertEquals(20, progressLog.length);
        for (int i = 0; i < 20; i++) {
            assertEquals("message_" + i, progressLog[i]);
        }
    }

    @Test
    public void testProgressLogCountMigrationFromOldJob() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(Job.PROPERTY_JOB_PROGRESS_LOG, new String[0]);

        final JobImpl job = new JobImpl("test", "hello_1", properties);

        assertTrue(job.getProperty(Job.PROPERTY_JOB_PROGRESS_LOG) instanceof String[]);

        for (int i = 0; i < 20; i++) {
            job.log(Integer.MAX_VALUE, "message_" + i);
        }

        final String[] progressLog = job.getProgressLog();
        assertEquals(20, progressLog.length);
        for (int i = 0; i < 20; i++) {
            assertEquals("message_" + i, progressLog[i]);
        }

        // now create a new Job with Max Count
        final JobImpl newJob = new JobImpl("test", "hello_1", properties);
        for (int i = 0; i < 20; i++) {
            newJob.log(10, "newMessage_" + i);
        }
        final String[] newProgressLog = newJob.getProgressLog();
        assertEquals(10, newProgressLog.length);
        for (int i = 0; i < 9; i++) {
            assertEquals("newMessage_1" + i, newProgressLog[i]);
        }
        assertEquals("newMessage_19" + JobImpl.TRUNCATED_LOG, newProgressLog[9]);
    }
}
