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
package org.apache.sling.event.impl.jobs.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.sling.event.jobs.TopicStatistics;
import org.junit.Test;

public class TopicStatisticsImplTest {

    @Test public void testInit() {
        final TopicStatistics s = new TopicStatisticsImpl("topic");
        assertEquals("topic", s.getTopic());
        assertEquals(0, s.getNumberOfFinishedJobs());
        assertEquals(0, s.getNumberOfCancelledJobs());
        assertEquals(0, s.getNumberOfFailedJobs());
        assertEquals(0, s.getNumberOfProcessedJobs());
        assertEquals(-1, s.getLastActivatedJobTime());
        assertEquals(-1, s.getLastFinishedJobTime());
        assertEquals(0, s.getAverageWaitingTime());
        assertEquals(0, s.getAverageProcessingTime());
    }

    @Test public void testJobFinished() {
        final TopicStatisticsImpl s = new TopicStatisticsImpl("topic");

        final long now = System.currentTimeMillis();

        s.addActive(500);
        s.addActive(700);

        s.finishedJob(300);
        s.finishedJob(500);

        assertEquals(2L, s.getNumberOfFinishedJobs());
        assertEquals(2L, s.getNumberOfProcessedJobs());

        assertEquals(400L, s.getAverageProcessingTime());
        assertEquals(600L, s.getAverageWaitingTime());
        assertTrue(s.getLastFinishedJobTime() >= now);
        assertTrue(s.getLastFinishedJobTime() <= System.currentTimeMillis());
        assertTrue(s.getLastActivatedJobTime() >= now);
        assertTrue(s.getLastActivatedJobTime() <= System.currentTimeMillis());
    }

    @Test public void testJobFailed() {
        final TopicStatisticsImpl s = new TopicStatisticsImpl("topic");

        final long now = System.currentTimeMillis();

        s.addActive(500);
        s.addActive(700);

        s.failedJob();
        s.failedJob();

        assertEquals(0L, s.getNumberOfFinishedJobs());
        assertEquals(2L, s.getNumberOfProcessedJobs());
        assertEquals(2L, s.getNumberOfFailedJobs());

        assertEquals(0, s.getAverageProcessingTime());
        assertEquals(600L, s.getAverageWaitingTime());
        assertEquals(-1L, s.getLastFinishedJobTime());
        assertTrue(s.getLastActivatedJobTime() >= now);
        assertTrue(s.getLastActivatedJobTime() <= System.currentTimeMillis());
    }

    @Test public void testJobCancelled() {
        final TopicStatisticsImpl s = new TopicStatisticsImpl("topic");

        final long now = System.currentTimeMillis();

        s.addActive(500);
        s.addActive(700);

        s.cancelledJob();
        s.cancelledJob();

        assertEquals(0L, s.getNumberOfFinishedJobs());
        assertEquals(2L, s.getNumberOfProcessedJobs());
        assertEquals(2L, s.getNumberOfCancelledJobs());

        assertEquals(0, s.getAverageProcessingTime());
        assertEquals(600L, s.getAverageWaitingTime());
        assertEquals(-1L, s.getLastFinishedJobTime());
        assertTrue(s.getLastActivatedJobTime() >= now);
        assertTrue(s.getLastActivatedJobTime() <= System.currentTimeMillis());
    }
}
