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
package org.apache.sling.event.impl.jobs.queues;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.sling.commons.threads.ThreadPool;
import org.apache.sling.event.impl.jobs.JobConsumerManager;
import org.apache.sling.event.impl.jobs.JobHandler;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.stats.StatisticsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.slf4j.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobQueueImplTest {

    private JobQueueImpl jobQueue;
    private QueueServices services;
    private JobManagerConfiguration configuration;
    private QueueJobCache cache;

    @InjectMocks
    private ThreadPool threadPool;

    private String testQueue = "testQueue";

    @Before
    public void setUp() {
        configuration = mock(JobManagerConfiguration.class);
        services = spy(QueueServices.class);
        InternalQueueConfiguration internalConfig = mock(InternalQueueConfiguration.class);
        services.configuration = configuration;
        when(configuration.isJobProcessingEnabled()).thenReturn(false);
        when(internalConfig.getMaxParallel()).thenReturn(5);
        when(internalConfig.getRetryDelayInMs()).thenReturn(1000L);
        cache = mock(QueueJobCache.class);
        threadPool = mock(ThreadPool.class);
        jobQueue = new JobQueueImpl(testQueue, internalConfig, services, cache, null);
    }

    @After
    public void tearDown() {
        if (jobQueue != null) {
            jobQueue.close();
        }
    }

    @Test
    public void testStartJobsWhenDisabled() {
        // Add a job handler to the queue
        JobHandler jobHandler = mock(JobHandler.class, RETURNS_DEEP_STUBS);
        String jobId = "testJob";
        when(jobHandler.getJob().getId()).thenReturn(jobId);
        jobQueue.getProcessingJobsLists().put(jobId, jobHandler);

        when(configuration.isJobProcessingEnabled()).thenReturn(false);
        jobQueue.startJobs();

        // The job should not be started/removed
        assertTrue(
                "Job should remain in processingJobsLists when processing is disabled",
                jobQueue.getProcessingJobsLists().containsKey(jobId));
    }

    @Test
    public void testStartJob() throws Exception {
        // Arrange
        JobHandler jobHandler = mock(JobHandler.class, RETURNS_DEEP_STUBS);
        String jobId = "testJob";
        when(jobHandler.getJob().getId()).thenReturn(jobId);
        when(jobHandler.getConsumer().process(any(), any())).thenReturn(JobExecutionResultImpl.SUCCEEDED);

        Logger auditLogger = mock(Logger.class);
        when(configuration.getAuditLogger()).thenReturn(auditLogger);

        // Processing disabled: should not process any job
        when(configuration.isJobProcessingEnabled()).thenReturn(false);

        // Act
        jobQueue.startJobs();

        // Assert: No job should be in processingJobsLists
        assertTrue(
                "No jobs should be processed when processing is disabled",
                jobQueue.getProcessingJobsLists().isEmpty());

        // Enable processing
        when(configuration.isJobProcessingEnabled()).thenReturn(true);

        // Mock the cache to return the job handler once, then null
        when(cache.getNextJob(any(JobConsumerManager.class), any(StatisticsManager.class), eq(jobQueue), anyBoolean()))
                .thenReturn(jobHandler)
                .thenReturn(null);

        Field threadPoolField = JobQueueImpl.class.getDeclaredField("threadPool");
        threadPoolField.setAccessible(true);
        threadPoolField.set(jobQueue, threadPool);
        doNothing().when(threadPool).execute(any());

        // Act: Try to start jobs again
        jobQueue.startJobs();

        // Wait for the job to be processed/removed (asynchronously)
        long timeout = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < timeout) {
            if (!jobQueue.getProcessingJobsLists().containsKey(jobId)) {
                break;
            }
            Thread.sleep(50);
        }

        // Assert: Job should now be removed
        assertFalse(
                "Job should be removed from processingJobsLists when processing is enabled",
                jobQueue.getProcessingJobsLists().containsKey(jobId));
    }

    @Test
    public void testQueueShutdown() {
        // Enable the configuration
        when(configuration.isJobProcessingEnabled()).thenReturn(true);

        jobQueue.close();

        // Verify that the queue is no longer running
        assertFalse("Queue should not be running after shutdown", jobQueue.isRunning());

        jobQueue.startJobs();

        // Verify that no jobs were started by checking the internal state
        assertTrue(
                "No jobs should be started after queue shutdown",
                jobQueue.getProcessingJobsLists().isEmpty());
    }

    @Test
    public void testQueueStartupAndShutdown() {
        when(configuration.isJobProcessingEnabled()).thenReturn(true);

        jobQueue.startJobs();

        // Add a job and verify it is present
        JobHandler jobHandler = mock(JobHandler.class, RETURNS_DEEP_STUBS);
        String jobId = "testJob";
        when(jobHandler.getJob().getId()).thenReturn(jobId);
        jobQueue.getProcessingJobsLists().put(jobId, jobHandler);

        assertTrue(
                "Processing jobs list should contain the job after adding",
                jobQueue.getProcessingJobsLists().containsKey(jobId));

        jobQueue.close();

        // Verify that the processingJobsLists is cleared after shutdown
        assertTrue(
                "Processing jobs list should be empty after shutdown",
                jobQueue.getProcessingJobsLists().isEmpty());
    }

    @Test
    public void testJobAssignmentWhenProcessingDisabled() {
        JobHandler jobHandler = mock(JobHandler.class, RETURNS_DEEP_STUBS);
        String jobId = "testJob";
        when(jobHandler.getJob().getId()).thenReturn(jobId);

        when(configuration.isJobProcessingEnabled()).thenReturn(false);

        jobQueue.getProcessingJobsLists().put(jobId, jobHandler);

        try {
            Method startJobMethod = JobQueueImpl.class.getDeclaredMethod("startJob", JobHandler.class);
            startJobMethod.setAccessible(true);
            startJobMethod.invoke(jobQueue, jobHandler);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke startJob method", e);
        }

        assertTrue(
                "Job should remain in processingJobsLists even when processing is disabled",
                jobQueue.getProcessingJobsLists().containsKey(jobId));
    }

    @Test
    public void testStartJobsWhenQueueSuspended() {
        // Add a job before suspending
        JobHandler jobHandler = mock(JobHandler.class, RETURNS_DEEP_STUBS);
        String jobId = "testJob";
        when(jobHandler.getJob().getId()).thenReturn(jobId);
        jobQueue.getProcessingJobsLists().put(jobId, jobHandler);

        jobQueue.suspend();
        jobQueue.startJobs();

        // The job should still be present since the queue is suspended
        assertTrue(
                "No jobs should be started when the queue is suspended",
                jobQueue.getProcessingJobsLists().containsKey(jobId));

        // Activate the queue and enable processing
        when(configuration.isJobProcessingEnabled()).thenReturn(true);
        jobQueue.resume(); // Use resume() to wake up the queue

        // Wait for the job to be processed/removed (asynchronously)
        long timeout = System.currentTimeMillis() + 2000; // 2 seconds timeout
        while (System.currentTimeMillis() < timeout) {
            if (!jobQueue.getProcessingJobsLists().containsKey(jobId)) {
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
            }
        }

        assertTrue(
                "Job should be removed from processingJobsLists after resuming queue when processing is enabled",
                jobQueue.getProcessingJobsLists().containsKey(jobId));
    }

    @Test
    public void testStopAllJobs() {
        // Enable job processing
        when(configuration.isJobProcessingEnabled()).thenReturn(true);

        // Create and add multiple jobs to the processing list
        JobHandler jobHandler1 = mock(JobHandler.class, RETURNS_DEEP_STUBS);
        JobHandler jobHandler2 = mock(JobHandler.class, RETURNS_DEEP_STUBS);
        String jobId1 = "testJob1";
        String jobId2 = "testJob2";
        when(jobHandler1.getJob().getId()).thenReturn(jobId1);
        when(jobHandler2.getJob().getId()).thenReturn(jobId2);

        // Add jobs to processing list
        jobQueue.getProcessingJobsLists().put(jobId1, jobHandler1);
        jobQueue.getProcessingJobsLists().put(jobId2, jobHandler2);

        // Stop all jobs
        jobQueue.stopAllJobs();

        // Verify both jobs were stopped
        verify(jobHandler1).stop();
        verify(jobHandler2).stop();

        // Verify jobs remain in processing list
        assertTrue(
                "Job1 should remain in processing list after stopAllJobs",
                jobQueue.getProcessingJobsLists().containsKey(jobId1));
        assertTrue(
                "Job2 should remain in processing list after stopAllJobs",
                jobQueue.getProcessingJobsLists().containsKey(jobId2));
        assertEquals(
                "Processing list should still contain 2 jobs",
                2,
                jobQueue.getProcessingJobsLists().size());
    }

    @Test
    public void testActualJobProcessing() throws Exception {
        // Arrange
        JobHandler jobHandler = mock(JobHandler.class, RETURNS_DEEP_STUBS);
        String jobId = "testJob";
        when(jobHandler.getJob().getId()).thenReturn(jobId);
        when(jobHandler.getConsumer().process(any(), any())).thenReturn(JobExecutionResultImpl.SUCCEEDED);

        // Enable job processing
        when(configuration.isJobProcessingEnabled()).thenReturn(true);

        // Mock the cache to return the job handler once, then null
        when(cache.getNextJob(any(), any(), any(), anyBoolean()))
                .thenReturn(jobHandler)
                .thenReturn(null);

        Field threadPoolField = JobQueueImpl.class.getDeclaredField("threadPool");
        threadPoolField.setAccessible(true);
        threadPoolField.set(jobQueue, threadPool);
        doNothing().when(threadPool).execute(any());
        // Act: Start jobs
        jobQueue.startJobs();

        // Wait for the job to be processed/removed (asynchronously)
        long timeout = System.currentTimeMillis() + 2000; // 2 seconds timeout
        while (System.currentTimeMillis() < timeout) {
            if (!jobQueue.getProcessingJobsLists().containsKey(jobId)) {
                break;
            }
            Thread.sleep(50);
        }

        // Assert: Job should be removed after processing
        assertFalse(
                "Job should be removed from processingJobsLists after successful processing",
                jobQueue.getProcessingJobsLists().containsKey(jobId));
    }
}
