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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.queues.JobQueueImpl;
import org.apache.sling.event.impl.jobs.queues.OutdatedJobQueueInfo;
import org.apache.sling.event.impl.jobs.queues.QueueJobCache;
import org.apache.sling.event.impl.jobs.queues.QueueServices;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class JobQueueImplTest {

    private JobQueueImpl jobQueue;
    private QueueServices services;
    private JobManagerConfiguration configuration;
    private Logger logger;
    private String testQueue = "testQueue";

    @Before
    public void setUp() {
        configuration = mock(JobManagerConfiguration.class);
        services = spy(QueueServices.class);
        logger = mock(Logger.class);
        InternalQueueConfiguration internalConfig = mock(InternalQueueConfiguration.class);
        services.configuration = configuration;
        when(configuration.isEnable()).thenReturn(false);
        when(internalConfig.getMaxParallel()).thenReturn(5);
        when(internalConfig.getRetryDelayInMs()).thenReturn(1000L);
        try {
            Constructor<JobQueueImpl> constructor = JobQueueImpl.class.getDeclaredConstructor(String.class, InternalQueueConfiguration.class, QueueServices.class, QueueJobCache.class, OutdatedJobQueueInfo.class);
            constructor.setAccessible(true);

            jobQueue = constructor.newInstance(testQueue, internalConfig, services, null, null);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        // Use reflection to set the logger field
        try {
            Field loggerField = JobQueueImpl.class.getDeclaredField("logger");
            loggerField.setAccessible(true);
            loggerField.set(jobQueue, logger);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testStartJobsWhenDisabled() {
        jobQueue.startJobs();

        verify(logger).debug("JobManager is disabled, skipping job starts for queue {}", testQueue);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void testStartJob() {
        // Mock a JobHandler and its behavior
        JobHandler jobHandler = mock(JobHandler.class, RETURNS_DEEP_STUBS);
        String jobId = "testJob";
        when(jobHandler.getJob().getId()).thenReturn(jobId);

        // Use reflection to access the private processingJobsLists field
        try {
            Field processingJobsListsField = JobQueueImpl.class.getDeclaredField("processingJobsLists");
            processingJobsListsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, JobHandler> processingJobsLists = (Map<String, JobHandler>) processingJobsListsField.get(jobQueue);
            processingJobsLists.put(testQueue, jobHandler);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access processingJobsLists field", e);
        }

        // Call the private startJob method using reflection
        try {
            Method startJobMethod = JobQueueImpl.class.getDeclaredMethod("startJob", JobHandler.class);
            startJobMethod.setAccessible(true);
            startJobMethod.invoke(jobQueue, jobHandler);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke startJob method", e);
        }

        // Verify the behavior
        verify(logger).debug("JobManager is disabled, stopping job {} in queue {}", jobId, testQueue);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void testQueueShutdown() {
        // Enable the configuration
        when(configuration.isEnable()).thenReturn(true);

        // Shut down the queue
        jobQueue.close();

        // Attempt to start jobs
        jobQueue.startJobs();

        // Verify that no jobs were started
        verify(logger).info("Stopped job queue {}", testQueue);
        verify(logger, never()).debug("Starting job queue {}", testQueue);
    }

    @Test
    public void testQueueStartupAndShutdown() {
        // Enable the configuration
        when(configuration.isEnable()).thenReturn(true);
        // Mock a valid QueueJobCache
        QueueJobCache mockCache = mock(QueueJobCache.class);
        try {
            Field cacheField = JobQueueImpl.class.getDeclaredField("cache");
            cacheField.setAccessible(true);
            cacheField.set(jobQueue, mockCache);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set mock cache", e);
        }

        // Start jobs
        jobQueue.startJobs();

        try {
            Field runningField = JobQueueImpl.class.getDeclaredField("running");
            runningField.setAccessible(true);
            boolean isRunning = (boolean) runningField.get(jobQueue);
            assertTrue(isRunning);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to access running field", e);
        }

        //stop the queue
        jobQueue.close();
        try {
            Field runningField = JobQueueImpl.class.getDeclaredField("running");
            runningField.setAccessible(true);
            boolean isRunning = (boolean) runningField.get(jobQueue);
            assertTrue(!isRunning);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to access running field", e);
        }
    }
}
