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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.felix.hc.api.Result;
import org.apache.felix.hc.api.execution.HealthCheckExecutionResult;
import org.apache.felix.hc.api.execution.HealthCheckExecutor;
import org.apache.felix.hc.api.execution.HealthCheckMetadata;
import org.apache.felix.hc.api.execution.HealthCheckSelector;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager.QueueInfo;
import org.apache.sling.event.impl.jobs.config.TopologyCapabilities;
import org.apache.sling.event.impl.support.ResourceHelper;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.sling.event.impl.EnvironmentComponent;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;

public class HealthCheckFailureTest {

    @Rule
    public final SlingContext context = new SlingContext();

    private JobManagerImpl jobManager;
    private JobManagerConfiguration configuration;
    private HealthCheckExecutor healthCheckExecutor;
    private MetricRegistry metricRegistry;
    private QueueConfigurationManager queueConfigManager;
    private TopologyCapabilities topologyCapabilities;
    private ResourceResolver resourceResolver;

    @Before
    public void setUp() {
        // Mock QueueConfigurationManager and QueueInfo
        queueConfigManager = mock(QueueConfigurationManager.class);
        QueueInfo queueInfo = new QueueInfo();
        queueInfo.queueName = "test-queue";
        when(queueConfigManager.getQueueInfo(any(String.class))).thenReturn(queueInfo);
        queueInfo.queueConfiguration = new InternalQueueConfiguration();

        // Mock TopologyCapabilities
        topologyCapabilities = mock(TopologyCapabilities.class);
        when(topologyCapabilities.detectTarget(any(String.class), any(Map.class), any(QueueInfo.class))).thenReturn(null);

        resourceResolver = mock(ResourceResolver.class);
        Resource resource = mock(Resource.class);
        when(resourceResolver.getResource(any(String.class))).thenReturn(resource);
        Resource parentResource = mock(Resource.class);

        // Mock resource creation
        when(resourceResolver.getResource(any(String.class))).thenReturn(resource);
        when(resourceResolver.getParent(any(Resource.class))).thenReturn(parentResource);
        when(resource.getResourceResolver()).thenReturn(resourceResolver);
        when(parentResource.getResourceResolver()).thenReturn(resourceResolver);
        try {
            when(resourceResolver.create(any(Resource.class), any(String.class), any(Map.class))).thenReturn(resource);
            doNothing().when(resourceResolver).commit();
        } catch (PersistenceException e) {
            throw new RuntimeException(e);
        }

        configuration = mock(JobManagerConfiguration.class);
        when(configuration.getScheduledJobsPath(false)).thenReturn("/var/sling/jobs");
        when(configuration.getQueueConfigurationManager()).thenReturn(queueConfigManager);
        when(configuration.getTopologyCapabilities()).thenReturn(topologyCapabilities);
        when(configuration.createResourceResolver()).thenReturn(resourceResolver);
        String path = "test/topic";
        when(configuration.getUniquePath(any(String.class), any(String.class), any(String.class), any(Map.class))).thenReturn(path);
        when(configuration.getUniquePath(any(), any(), any(), any())).thenReturn("/");
        when(configuration.getUniqueId(any())).thenReturn("1");
        when(configuration.getAuditLogger()).thenReturn(mock(Logger.class));
        // Mock the static method
        try (MockedStatic<ResourceHelper> mockedStatic = mockStatic(ResourceHelper.class)) {
            // Define the behavior of the static method
            Resource mockResource = mock(Resource.class);
            mockedStatic.when(() -> ResourceHelper.createAndCommitResource(any(ResourceResolver.class), anyString(), anyMap()))
                    .thenReturn(mockResource);
        }
        context.registerService(JobManagerConfiguration.class, configuration);

        // Mock and register other required services
        healthCheckExecutor = mock(HealthCheckExecutor.class);
        metricRegistry = new MetricRegistry();
        context.registerService(HealthCheckExecutor.class, healthCheckExecutor);
        context.registerService(MetricRegistry.class, metricRegistry);

        // Register other dependencies
        context.registerService(org.apache.sling.commons.scheduler.Scheduler.class, mock(org.apache.sling.commons.scheduler.Scheduler.class));
        context.registerService(org.apache.sling.commons.threads.ThreadPoolManager.class, mock(org.apache.sling.commons.threads.ThreadPoolManager.class));
        context.registerService(org.apache.sling.event.jobs.jmx.QueuesMBean.class, mock(org.apache.sling.event.jobs.jmx.QueuesMBean.class));
        context.registerService(org.apache.sling.event.impl.jobs.stats.StatisticsManager.class, mock(org.apache.sling.event.impl.jobs.stats.StatisticsManager.class));
        context.registerService(org.apache.sling.event.impl.jobs.JobConsumerManager.class, mock(org.apache.sling.event.impl.jobs.JobConsumerManager.class));
        context.registerService(org.apache.sling.event.impl.jobs.queues.QueueManager.class, mock(org.apache.sling.event.impl.jobs.queues.QueueManager.class));
        context.registerService(EnvironmentComponent.class, mock(EnvironmentComponent.class));

        // Create and register JobManagerImpl
        jobManager = new JobManagerImpl();
        context.registerInjectActivateService(jobManager);
    }

    @Test
    public void testHealthCheckFailure() {
        // Mock HealthCheckExecutor to simulate failure
        when(healthCheckExecutor.execute(any(HealthCheckSelector.class))).thenAnswer(new Answer<List<HealthCheckExecutionResult>>() {
            @Override
            public List<HealthCheckExecutionResult> answer(InvocationOnMock invocation) {
                HealthCheckExecutionResult failedResult = mock(HealthCheckExecutionResult.class);
                Result result = mock(Result.class);
                HealthCheckMetadata metadata = mock(HealthCheckMetadata.class);
                
                when(result.getStatus()).thenReturn(Result.Status.WARN);
                when(failedResult.getHealthCheckMetadata()).thenReturn(metadata);
                when(metadata.getName()).thenReturn("Test Health Check");
                when(failedResult.getHealthCheckResult()).thenReturn(result);
                
                return Collections.singletonList(failedResult);
            }
        });

        // Create a job that should be affected by the health check
        Map<String, Object> properties = new HashMap<>();
        properties.put("testProperty", "testValue");
        
        // Try to add a job when system is unhealthy
        Job job = jobManager.addJob("test/topic", properties);
        
        // Verify that the job was not added due to health check failure
        assertNull("Job should not be added when system is unhealthy", job);
    }

    @Test
    public void testHealthCheckRecovery() {
        // First simulate a failure
        when(healthCheckExecutor.execute(any(HealthCheckSelector.class))).thenAnswer(new Answer<List<HealthCheckExecutionResult>>() {
            private boolean firstCall = true;
            
            @Override
            public List<HealthCheckExecutionResult> answer(InvocationOnMock invocation) {
                HealthCheckExecutionResult result = mock(HealthCheckExecutionResult.class);
                Result healthResult = mock(Result.class);
                HealthCheckMetadata metadata = mock(HealthCheckMetadata.class);
                
                if (firstCall) {
                    // First call returns WARN
                    when(healthResult.getStatus()).thenReturn(Result.Status.WARN);
                    firstCall = false;
                } else {
                    // Subsequent calls return OK
                    when(healthResult.getStatus()).thenReturn(Result.Status.OK);
                }

                when(result.getHealthCheckMetadata()).thenReturn(metadata);
                when(metadata.getName()).thenReturn("Test Health Check");
                when(result.getHealthCheckResult()).thenReturn(healthResult);
                
                return Collections.singletonList(result);
            }
        });

        // First attempt should fail
        Map<String, Object> properties = new HashMap<>();
        properties.put("testProperty", "testValue");
        Job job = jobManager.addJob("test/topic", properties);
        assertNull("Job should not be added when system is unhealthy", job);

        // Second attempt should succeed after health check recovery
        job = jobManager.addJob("test/topic", properties);
        assertNotNull("Job should not be null after health check recovery", job);
    }
} 
