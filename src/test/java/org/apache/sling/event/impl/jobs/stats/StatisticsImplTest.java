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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StatisticsImplTest {

    @Test
    public void testInitNoStartTime() {
        final long now = System.currentTimeMillis();
        final StatisticsImpl s = new StatisticsImpl();
        assertTrue(s.getStartTime() >= now);
        assertTrue(s.getStartTime() <= System.currentTimeMillis());
        assertEquals(0, s.getNumberOfFinishedJobs());
        assertEquals(0, s.getNumberOfCancelledJobs());
        assertEquals(0, s.getNumberOfFailedJobs());
        assertEquals(0, s.getNumberOfProcessedJobs());
        assertEquals(0, s.getNumberOfActiveJobs());
        assertEquals(0, s.getNumberOfQueuedJobs());
        assertEquals(0, s.getNumberOfJobs());
        assertEquals(-1, s.getLastActivatedJobTime());
        assertEquals(-1, s.getLastFinishedJobTime());
        assertEquals(0, s.getAverageWaitingTime());
        assertEquals(0, s.getAverageProcessingTime());
    }

    @Test
    public void testInitStartTime() {
        final StatisticsImpl s = new StatisticsImpl(7000);
        assertEquals(7000L, s.getStartTime());
        assertEquals(0, s.getNumberOfFinishedJobs());
        assertEquals(0, s.getNumberOfCancelledJobs());
        assertEquals(0, s.getNumberOfFailedJobs());
        assertEquals(0, s.getNumberOfProcessedJobs());
        assertEquals(0, s.getNumberOfActiveJobs());
        assertEquals(0, s.getNumberOfQueuedJobs());
        assertEquals(0, s.getNumberOfJobs());
        assertEquals(-1, s.getLastActivatedJobTime());
        assertEquals(-1, s.getLastFinishedJobTime());
        assertEquals(0, s.getAverageWaitingTime());
        assertEquals(0, s.getAverageProcessingTime());
    }

    @Test
    public void reset() {
        final StatisticsImpl s = new StatisticsImpl(7000);
        final long now = System.currentTimeMillis();
        s.reset();
        assertTrue(s.getStartTime() >= now);
        assertTrue(s.getStartTime() <= System.currentTimeMillis());
        assertEquals(0, s.getNumberOfFinishedJobs());
        assertEquals(0, s.getNumberOfCancelledJobs());
        assertEquals(0, s.getNumberOfFailedJobs());
        assertEquals(0, s.getNumberOfProcessedJobs());
        assertEquals(0, s.getNumberOfActiveJobs());
        assertEquals(0, s.getNumberOfQueuedJobs());
        assertEquals(0, s.getNumberOfJobs());
        assertEquals(-1, s.getLastActivatedJobTime());
        assertEquals(-1, s.getLastFinishedJobTime());
        assertEquals(0, s.getAverageWaitingTime());
        assertEquals(0, s.getAverageProcessingTime());
    }

    @Test
    public void testJobFinished() {
        final StatisticsImpl s = new StatisticsImpl();

        final long now = System.currentTimeMillis();

        s.incQueued();
        s.incQueued();
        assertEquals(2L, s.getNumberOfQueuedJobs());
        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.addActive(500);
        s.addActive(700);
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(2L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.finishedJob(300);
        s.finishedJob(500);

        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(2L, s.getNumberOfFinishedJobs());
        assertEquals(2L, s.getNumberOfProcessedJobs());
        assertEquals(0L, s.getNumberOfJobs());

        assertEquals(400L, s.getAverageProcessingTime());
        assertEquals(600L, s.getAverageWaitingTime());
        assertTrue(s.getLastFinishedJobTime() >= now);
        assertTrue(s.getLastFinishedJobTime() <= System.currentTimeMillis());
        assertTrue(s.getLastActivatedJobTime() >= now);
        assertTrue(s.getLastActivatedJobTime() <= System.currentTimeMillis());
    }

    @Test
    public void testJobFailed() {
        final StatisticsImpl s = new StatisticsImpl();

        final long now = System.currentTimeMillis();

        s.incQueued();
        s.incQueued();
        assertEquals(2L, s.getNumberOfQueuedJobs());
        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.addActive(500);
        s.addActive(700);
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(2L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.failedJob();
        s.failedJob();

        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(0L, s.getNumberOfFinishedJobs());
        assertEquals(2L, s.getNumberOfProcessedJobs());
        assertEquals(2L, s.getNumberOfFailedJobs());
        assertEquals(0L, s.getNumberOfJobs());

        assertEquals(0, s.getAverageProcessingTime());
        assertEquals(600L, s.getAverageWaitingTime());
        assertEquals(-1L, s.getLastFinishedJobTime());
        assertTrue(s.getLastActivatedJobTime() >= now);
        assertTrue(s.getLastActivatedJobTime() <= System.currentTimeMillis());
    }

    @Test
    public void testJobCancelled() {
        final StatisticsImpl s = new StatisticsImpl();

        final long now = System.currentTimeMillis();

        s.incQueued();
        s.incQueued();
        assertEquals(2L, s.getNumberOfQueuedJobs());
        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.addActive(500);
        s.addActive(700);
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(2L, s.getNumberOfActiveJobs());
        assertEquals(2L, s.getNumberOfJobs());

        s.cancelledJob();
        s.cancelledJob();

        assertEquals(0L, s.getNumberOfActiveJobs());
        assertEquals(0L, s.getNumberOfQueuedJobs());
        assertEquals(0L, s.getNumberOfFinishedJobs());
        assertEquals(2L, s.getNumberOfProcessedJobs());
        assertEquals(2L, s.getNumberOfCancelledJobs());
        assertEquals(0L, s.getNumberOfJobs());

        assertEquals(0, s.getAverageProcessingTime());
        assertEquals(600L, s.getAverageWaitingTime());
        assertEquals(-1L, s.getLastFinishedJobTime());
        assertTrue(s.getLastActivatedJobTime() >= now);
        assertTrue(s.getLastActivatedJobTime() <= System.currentTimeMillis());
    }
}
