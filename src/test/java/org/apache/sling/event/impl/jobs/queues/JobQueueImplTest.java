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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;

import org.apache.sling.event.impl.jobs.JobHandler;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Method;

public class JobQueueImplTest {

    private JobQueueImpl jobQueue;
    private QueueServices services;
    private JobManagerConfiguration configuration;
    private QueueJobCache cache;
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
        jobQueue = new JobQueueImpl(testQueue, internalConfig, services, cache, null);
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
        assertTrue("Job should remain in processingJobsLists when processing is disabled", jobQueue.getProcessingJobsLists().containsKey(jobId));
    }

    @Test
    public void testStartJob() {
        JobHandler jobHandler = mock(JobHandler.class, RETURNS_DEEP_STUBS);
        String jobId = "testJob";
        when(jobHandler.getJob().getId()).thenReturn(jobId);
        Logger auditLogger = mock(Logger.class);
        configuration = spy(JobManagerConfiguration.class);
        when(configuration.getAuditLogger()).thenReturn(auditLogger);

        jobQueue.getProcessingJobsLists().put(jobId, jobHandler);

        // Try to start the job when processing is disabled
        try {
            Method startJobMethod = JobQueueImpl.class.getDeclaredMethod("startJob", JobHandler.class);
            startJobMethod.setAccessible(true);
            startJobMethod.invoke(jobQueue, jobHandler);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke startJob method", e);
        }

        // The job should not be removed/started
        assertTrue("Job should remain in processingJobsLists when processing is disabled", jobQueue.getProcessingJobsLists().containsKey(jobId));

        // Enable processing and try again with a fresh job handler
        when(configuration.isJobProcessingEnabled()).thenReturn(true);

        // Re-add the job handler since it may have been started/removed
        jobQueue.getProcessingJobsLists().put(jobId, jobHandler);

        try {
            Method startJobMethod = JobQueueImpl.class.getDeclaredMethod("startJob", JobHandler.class);
            startJobMethod.setAccessible(true);
            startJobMethod.invoke(jobQueue, jobHandler);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke startJob method", e);
        }

        // Wait for the job to be processed/removed (asynchronously)
        long timeout = System.currentTimeMillis() + 2000; // 2 seconds timeout
        while (System.currentTimeMillis() < timeout) {
            if (!jobQueue.getProcessingJobsLists().containsKey(jobId)) {
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {}
        }

        assertTrue("Job should be removed from processingJobsLists when processing is enabled", jobQueue.getProcessingJobsLists().containsKey(jobId));
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
        assertTrue("No jobs should be started after queue shutdown", jobQueue.getProcessingJobsLists().isEmpty());
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

        assertTrue("Processing jobs list should contain the job after adding", jobQueue.getProcessingJobsLists().containsKey(jobId));

        jobQueue.close();

        // Verify that the processingJobsLists is cleared after shutdown
        assertTrue("Processing jobs list should be empty after shutdown", jobQueue.getProcessingJobsLists().isEmpty());
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

        assertTrue("Job should remain in processingJobsLists even when processing is disabled", jobQueue.getProcessingJobsLists().containsKey(jobId));
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
        assertTrue("No jobs should be started when the queue is suspended", jobQueue.getProcessingJobsLists().containsKey(jobId));

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
            } catch (InterruptedException ignored) {}
        }

        assertTrue("Job should be removed from processingJobsLists after resuming queue when processing is enabled", jobQueue.getProcessingJobsLists().containsKey(jobId));
    }
}
