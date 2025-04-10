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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.felix.hc.api.Result;
import org.apache.felix.hc.api.execution.HealthCheckExecutionResult;
import org.apache.felix.hc.api.execution.HealthCheckExecutor;
import org.apache.felix.hc.api.execution.HealthCheckSelector;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.commons.scheduler.Scheduler;
import org.apache.sling.commons.threads.ThreadPoolManager;
import org.apache.sling.event.impl.jobs.JobConsumerManager;
import org.apache.sling.event.impl.jobs.JobHandler;
import org.apache.sling.event.impl.jobs.JobImpl;
import org.apache.sling.event.impl.jobs.JobManagerImpl;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager.QueueInfo;
import org.apache.sling.event.impl.jobs.config.TopologyCapabilities;
import org.apache.sling.event.impl.jobs.stats.StatisticsManager;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.event.jobs.consumer.JobExecutionContext;
import org.apache.sling.event.jobs.consumer.JobExecutionResult;
import org.apache.sling.event.jobs.consumer.JobExecutor;
import org.apache.sling.event.jobs.jmx.QueuesMBean;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

public class JobExecutionContextImplTest {

    @Rule
    public SlingContext context = new SlingContext();

    private JobManager jobManager;
    private JobManagerConfiguration configuration;

    @Before
    public void setUp() {
        configuration = createMockJobManagerConfiguration();

        QueueConfigurationManager queueConfigMgr = mock(QueueConfigurationManager.class);
        QueueInfo info = new QueueInfo();
        info.queueConfiguration = new InternalQueueConfiguration();
        when(queueConfigMgr.getQueueInfo(anyString())).thenReturn(info);
        when(configuration.getQueueConfigurationManager()).thenReturn(queueConfigMgr);

        TopologyCapabilities capabilities = mock(TopologyCapabilities.class);
        JobConsumerManager jobConsumerManager = mock(JobConsumerManager.class);
        QueueManager qManager = mock(QueueManager.class);
        ThreadPoolManager threadPoolManager = mock(ThreadPoolManager.class);
        MetricRegistry metric = mock(MetricRegistry.class);
        StatisticsManager statisticsManager = mock(StatisticsManager.class);
        QueuesMBean queuesMBean = mock(QueuesMBean.class);
        Scheduler scheduler = mock(Scheduler.class);
        HealthCheckExecutor healthCheckExecutor = mock(HealthCheckExecutor.class);

        // Mock health check to return OK status
        when(healthCheckExecutor.execute(any(HealthCheckSelector.class))).thenAnswer(new Answer<List<HealthCheckExecutionResult>>() {
            @Override
            public List<HealthCheckExecutionResult> answer(InvocationOnMock invocation) {
                HealthCheckExecutionResult result = mock(HealthCheckExecutionResult.class);
                Result healthResult = mock(Result.class);
                when(healthResult.getStatus()).thenReturn(Result.Status.OK);
                when(result.getHealthCheckResult()).thenReturn(healthResult);
                return Collections.singletonList(result);
            }
        });

        context.registerService(JobManagerConfiguration.class, configuration);
        context.registerService(TopologyCapabilities.class, capabilities);
        context.registerService(QueueConfigurationManager.class, queueConfigMgr);
        context.registerService(MetricRegistry.class, metric);
        context.registerService(ThreadPoolManager.class, threadPoolManager);
        context.registerService(QueueManager.class, qManager);
        context.registerService(JobConsumerManager.class, jobConsumerManager);
        context.registerService(StatisticsManager.class, statisticsManager);
        context.registerService(QueuesMBean.class, queuesMBean);
        context.registerService(Scheduler.class, scheduler);
        context.registerService(HealthCheckExecutor.class, healthCheckExecutor, new HashMap<String, Object>() {{
            put("service.pid", "healthCheckExecutor");
        }});
        context.registerService(JobExecutor.class, new TestJobExecutor(), new HashMap<String, Object>() {{
            put(JobExecutor.PROPERTY_TOPICS, "test");
        }});

        jobManager = new JobManagerImpl();
        context.registerInjectActivateService(jobManager, new HashMap<String, Object>());
    }

    @Test
    public void testSetProperty() {
        // Create a job - it will be written to the mock jcr
        Job job = jobManager.addJob("test", null);

        // Process the job
        JobExecutor je = new TestJobExecutor();
        je.process(job, new JobExecutionContextImpl(new JobHandler((JobImpl) job, je, configuration), null));

        // Retrieve the custom property
        assertEquals("testValue", job.getProperty("test", String.class));

        final String testValue;
        Iterable<Resource> resources = context.resourceResolver().getResource("/var/eventing/jobs/assigned").getChildren();
        ValueMap props = resources.iterator().next().adaptTo(ValueMap.class);
        testValue = props.get("test", String.class);
        assertEquals("testValue", testValue);
    }

    public class TestJobExecutor implements JobExecutor {

        @Override
        public JobExecutionResult process(Job job, JobExecutionContext context) {
            context.setProperty("test", "testValue");
            return context.result().message("TEST").succeeded();
        }
    }

    private JobManagerConfiguration createMockJobManagerConfiguration() {
        JobManagerConfiguration jobManagerConfig = mock(JobManagerConfiguration.class);

        String jobsPath = "/var/eventing";

        when(jobManagerConfig.getUniqueId(anyString())).then(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                byte [] digest = java.security.MessageDigest.getInstance("md5").digest(String.valueOf(Math.random()).getBytes("UTF-8"));
                BigInteger bigInt = new BigInteger(1, digest);
                String hashtext = bigInt.toString(16);
                return hashtext + "_" + String.valueOf((int)(Math.random()* 1000000));
            }
        });
        when(jobManagerConfig.getScheduledJobsPath(false)).thenReturn(jobsPath + "/scheduled-jobs");
        when(jobManagerConfig.getUniquePath(eq(null), anyString(), anyString(), eq(null))).then(
            new Answer<String>() {
                @Override
                public String answer(InvocationOnMock invocation) throws Throwable {
                    String jobNodePath = jobsPath + "/jobs/assigned/" + invocation.getArgument(2);
                    return jobNodePath;
                }
            }
        );
        when(jobManagerConfig.getAuditLogger()).thenReturn(LoggerFactory.getLogger("org.apache.sling.event.jobs.audit"));
        ResourceResolver resolver = context.resourceResolver();
        when(jobManagerConfig.createResourceResolver()).thenReturn(resolver);
        return jobManagerConfig;
    }
}
