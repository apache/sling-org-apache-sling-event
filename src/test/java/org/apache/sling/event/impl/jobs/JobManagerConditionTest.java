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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sling.commons.scheduler.Scheduler;
import org.apache.sling.commons.threads.ThreadPool;
import org.apache.sling.commons.threads.ThreadPoolManager;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager;
import org.apache.sling.event.impl.jobs.config.TopologyCapabilities;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration;
import org.apache.sling.event.impl.support.Environment;
import org.apache.sling.event.jobs.jmx.QueuesMBean;
import org.apache.sling.event.impl.jobs.queues.QueueManager;
import org.apache.sling.event.impl.jobs.stats.StatisticsManager;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.Job.JobState;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.osgi.service.condition.Condition;
import org.osgi.service.event.EventAdmin;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class JobManagerConditionTest {

    @Rule
    public SlingContext context = new SlingContext();

    @Mock
    private JobManagerConfiguration configuration;

    @Mock
    private EventAdmin eventAdmin;

    @Mock
    private Condition condition;

    @Mock
    private JobConsumerManager jobConsumerManager;

    @Mock
    private QueuesMBean queuesMBean;

    @Mock
    private ThreadPoolManager threadPoolManager;

    @Mock
    private ThreadPool threadPool;

    @Mock
    private StatisticsManager statisticsManager;

    @Mock
    private QueueConfigurationManager queueConfigMgr;

    @Mock
    private TopologyCapabilities capabilities;

    @Mock
    private Scheduler scheduler;

    @Mock
    private MetricRegistry metricRegistry;

    private QueueManager queueManager;
    private JobManagerImpl jobManager;
    private AtomicInteger processedJobs;
    private Logger logger;

    @Before
    public void setup() throws Exception {
        processedJobs = new AtomicInteger(0);

        // Mock required configuration values
        InternalQueueConfiguration mockConfig = mock(InternalQueueConfiguration.class);
        when(configuration.getQueueConfigurationManager()).thenReturn(queueConfigMgr);
        when(configuration.getTopologyCapabilities()).thenReturn(capabilities);

        queueConfigMgr = mock(QueueConfigurationManager.class);
        when(configuration.getQueueConfigurationManager()).thenReturn(queueConfigMgr);

        // Mock the QueueInfo
        queueConfigMgr = mock(QueueConfigurationManager.class);
        QueueConfigurationManager.QueueInfo info = new QueueConfigurationManager.QueueInfo();
        info.queueConfiguration = new InternalQueueConfiguration();
        when(queueConfigMgr.getQueueInfo(anyString())).thenReturn(info);
        when(configuration.getQueueConfigurationManager()).thenReturn(queueConfigMgr);

        when(configuration.getUniqueId(anyString())).then(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                return String.valueOf(System.currentTimeMillis());
            }
        });
        when(configuration.getUniquePath(any(), any(), any(), any())).then(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                String jobId = invocation.getArgument(2);
                return "/var/eventing/jobs/assigned/" + jobId;
            }
        });
        when(configuration.getScheduledJobsPath(false)).thenReturn("/var/eventing/scheduled-jobs");
        when(configuration.getAuditLogger()).thenReturn(LoggerFactory.getLogger("org.apache.sling.event.jobs.audit"));
        when(configuration.createResourceResolver()).thenReturn(context.resourceResolver());

        when(configuration.isEnable()).thenReturn(true);

        // Mock Environment.APPLICATION_ID
        Field appIdField = Environment.class.getDeclaredField("APPLICATION_ID");
        appIdField.setAccessible(true);
        appIdField.set(null, "testAppId");
        // Mock assignedJobsPath
        Field assignedJobsPathField = JobManagerConfiguration.class.getDeclaredField("assignedJobsPath");
        assignedJobsPathField.setAccessible(true);
        assignedJobsPathField.set(configuration, "/mock/assignedJobs");

        Scheduler scheduler = mock(Scheduler.class);

        // Register required services
        context.registerService(JobManagerConfiguration.class, configuration);
        context.registerService(TopologyCapabilities.class, capabilities);
        context.registerService(QueueConfigurationManager.class, queueConfigMgr);
        context.registerService(MetricRegistry.class, metricRegistry);
        context.registerService(JobConsumerManager.class, jobConsumerManager);
        context.registerService(ThreadPoolManager.class, threadPoolManager);
        context.registerService(StatisticsManager.class, statisticsManager);
        context.registerService(QueuesMBean.class, queuesMBean);
        context.registerService(Scheduler.class, scheduler);
        context.registerService(Condition.class, condition, new HashMap<String, Object>() {{
            put("osgi.condition.id", "true");
        }});

        // Setup queue manager
        queueManager = QueueManager.newForTest(eventAdmin, jobConsumerManager, queuesMBean,
                threadPoolManager, threadPool, configuration, statisticsManager);
        context.registerService(QueueManager.class, queueManager);

        // Setup job manager
        jobManager = new JobManagerImpl();
        context.registerInjectActivateService(jobManager, new HashMap<String, Object>());
    }

    @Test
    public void testJobProcessingWithCondition() {
        // Register a test consumer
        JobConsumer consumer = job -> {
            processedJobs.incrementAndGet();
            return JobConsumer.JobResult.OK;
        };
        context.registerService(JobConsumer.class, consumer, new HashMap<String, Object>() {{
            put(JobConsumer.PROPERTY_TOPICS, "test/topic");
        }});

        // Add a job
        Job job = jobManager.addJob("test/topic", new HashMap<>());
        assertTrue("Job should be created", job != null);

        assertEquals("Job should be in QUEUED state", JobState.QUEUED, job.getJobState());
        jobManager.deactivate();
    }

    @Test
    public void testQueueMaintenanceWithCondition() {
        // Start with condition
        when(configuration.isEnable()).thenReturn(true);

        // Run maintenance
        queueManager.run();
        verify(configuration).isEnable();

        // Remove condition
        when(configuration.isEnable()).thenReturn(false);

        // Run maintenance again
        queueManager.run();
        verify(configuration,times(2)).isEnable();
        jobManager.deactivate();
    }
} 
