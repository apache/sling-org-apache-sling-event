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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jcr.ItemExistsException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.version.VersionException;

import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.scheduler.Scheduler;
import org.apache.sling.commons.threads.ThreadPool;
import org.apache.sling.commons.threads.ThreadPoolManager;
import org.apache.sling.event.impl.jobs.JobConsumerManager;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration.Config;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.config.JobManagerConfigurationTestFactory;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager.QueueInfo;
import org.apache.sling.event.impl.jobs.jmx.QueuesMBeanImpl;
import org.apache.sling.event.impl.jobs.scheduling.JobSchedulerImpl;
import org.apache.sling.event.impl.jobs.stats.StatisticsManager;
import org.apache.sling.event.impl.support.Environment;
import org.apache.sling.event.impl.support.ResourceHelper;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.Queue;
import org.apache.sling.event.jobs.QueueConfiguration.ThreadPriority;
import org.apache.sling.event.jobs.QueueConfiguration.Type;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.sling.MockSling;
import org.apache.sling.testing.mock.sling.ResourceResolverType;
import org.apache.sling.testing.mock.sling.builder.ContentBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.event.EventAdmin;

@RunWith(MockitoJUnitRunner.class)
public class TestQueueJobCache {

    private static AtomicInteger cnfeCount = new AtomicInteger();

    @Mock
    private JobSchedulerImpl jobScheduler;
    
    @Mock
    private EventAdmin eventAdmin;
    
    @Mock
    private JobConsumerManager jobConsumerManager;

    @Mock
    private ThreadPoolManager threadPoolManager;

    @Mock
    private ThreadPool threadPool;

    @Mock
    private StatisticsManager statisticsManager;

    @Mock
    private QueueConfigurationManager queueConfigurationManager;

    @Mock
    private Scheduler scheduler;

    private JobManagerConfiguration configuration;

    private QueuesMBeanImpl queuesMBean;
    
    private String ownSlingId;

    private int jobCnt;

    private ResourceResolverFactory factory;

    private ComponentContext componentContext;

    private BundleContext bundleContext;

    /** object under test */
    private QueueManager queueManager;

    @Before
    public void setUp() throws Throwable {
        cnfeCount.set(0);
        ownSlingId = UUID.randomUUID().toString();
        Environment.APPLICATION_ID = ownSlingId;
        componentContext = MockOsgi.newComponentContext();
        bundleContext = componentContext.getBundleContext();
        
        factory = MockSling.newResourceResolverFactory(ResourceResolverType.JCR_OAK, bundleContext);

        queuesMBean = new QueuesMBeanImpl();
        queuesMBean.activate(bundleContext);

        configuration = JobManagerConfigurationTestFactory.create(JobManagerConfiguration.DEFAULT_REPOSITORY_PATH, 
                factory, queueConfigurationManager);
        
        queueManager = QueueManager.newForTest(eventAdmin, jobConsumerManager, 
                queuesMBean, threadPoolManager, threadPool, configuration, statisticsManager);

        initQueueConfigurationManagerMocks();
        
        queueManager.activate(null);

        @SuppressWarnings("deprecation")
        ResourceResolver resourceResolver = factory.getAdministrativeResourceResolver(null);
        ContentBuilder contentBuilder = new ContentBuilder(resourceResolver);

        final String topic = "aTopic";
        for (int year = 2019; year <= 2022; year++) {
            for (int month = 1; month <= 12; month++) {
                createJob(contentBuilder, ownSlingId, topic, year, month);
            }
        }
        resourceResolver.commit();
    }

    private void initQueueConfigurationManagerMocks() {
        Mockito.when(queueConfigurationManager.getQueueInfo(Mockito.anyString())).thenAnswer(new Answer<QueueInfo>() {
            
            private final Map<String, QueueInfo> queueInfos = new HashMap<>();

            @Override
            public QueueInfo answer(InvocationOnMock invocation) throws Throwable {
                final String topic = (String) invocation.getArguments()[0];
                QueueInfo queueInfo = queueInfos.get(topic);
                if ( queueInfo == null ) {
                    queueInfo = createQueueInfo(topic);
                    queueInfos.put(topic, queueInfo);
                }
                return queueInfo;
            }

            private QueueInfo createQueueInfo(final String topic) {
                final QueueInfo result = new QueueInfo();
                result.queueName = "Queue for topic=" + topic;
                Map<String, Object> props = new HashMap<>();
                Config cconfig = Mockito.mock(Config.class);
                Mockito.when(cconfig.queue_priority()).thenReturn(ThreadPriority.NORM.name());
                Mockito.when(cconfig.queue_type()).thenReturn(Type.ORDERED.name());
                Mockito.when(cconfig.queue_maxparallel()).thenReturn(1.0);
                result.queueConfiguration = InternalQueueConfiguration.fromConfiguration(props, cconfig);
                result.targetId = ownSlingId;
                return result;
            }
            
        });
    }
    
    @Test
    public void testUnhalting() throws Throwable {
        assertNotNull(queueManager);
        assertEquals(0, cnfeCount.get());
        queueManager.configurationChanged(true);
        int expectedCnfeCount = 2 * jobCnt;
        assertEquals(expectedCnfeCount, cnfeCount.get());
        queueManager.maintain();
        assertEquals(expectedCnfeCount, cnfeCount.get());
        queueManager.maintain();
        assertEquals(expectedCnfeCount, cnfeCount.get());
        queueManager.maintain();
        assertEquals(expectedCnfeCount, cnfeCount.get());
        queueManager.configurationChanged(true);
        expectedCnfeCount += 2 * jobCnt;
        assertEquals(expectedCnfeCount, cnfeCount.get());
        queueManager.maintain();
        assertEquals(expectedCnfeCount, cnfeCount.get());
        queueManager.maintain();
        assertEquals(expectedCnfeCount, cnfeCount.get());
        queueManager.maintain();
        assertEquals(expectedCnfeCount, cnfeCount.get());
    }

    @Test
    public void testFullTopicScan() throws Throwable {
        assertNotNull(queueManager);
        assertEquals(0, cnfeCount.get());
        for( int i = 0; i < 50; i++ ) {
            queueManager.fullTopicScan();
            assertEquals(2 * jobCnt, cnfeCount.get());
        }
    }

    @Test
    public void testMaintain() throws Throwable {
        assertNotNull(queueManager);
        assertEquals(0, cnfeCount.get());
        // configurationChanged(true) is going to do a fullTopicScan()
        queueManager.configurationChanged(true);
        // due to fullTopicScan -> 2 times the jobs are loaded causing a CNFE
        int expectedCnfeCount = 2 * jobCnt;
        assertEquals(expectedCnfeCount, cnfeCount.get());
        for( int i = 0; i < 50; i++ ) {
            // schedulerRuns % 3 == 1
            queueManager.maintain();
            assertEquals(expectedCnfeCount, cnfeCount.get());
            // schedulerRuns % 3 == 2
            queueManager.maintain();
            assertEquals(expectedCnfeCount, cnfeCount.get());
            // schedulerRuns % 3 == 0
            queueManager.maintain();
            // if we weren't halting the topic due to CNFE, we'd now be doing:
            // expectedCnfeCount += 2 * jobCnt;
            assertEquals(expectedCnfeCount, cnfeCount.get());
        }
    }
    
    @Test
    public void testConfigurationChanged() throws Throwable {
        assertNotNull(queueManager);
        assertEquals(0, cnfeCount.get());
        queueManager.configurationChanged(false);
        assertEquals(0, cnfeCount.get());
        queueManager.configurationChanged(true);
        assertEquals(2 * jobCnt, cnfeCount.get());
        Iterable<Queue> qit = queueManager.getQueues();
        assertNotNull(qit);
        Iterator<Queue> it = qit.iterator();
        assertNotNull(it);
        assertFalse(it.hasNext());
    }
    
    private Resource createJob(ContentBuilder contentBuilder, String localSlingId, String topic, int year, int month) throws ItemExistsException, PathNotFoundException, VersionException, ConstraintViolationException, LockException, RepositoryException {
        // /var/eventing/jobs/assigned/<slingId>/<topic>/2020/10/13/19/26        
        String applicationId = localSlingId;
        String counter = String.valueOf(jobCnt++);
        String jobId = year + "/" + month + "/1/20/0/" + applicationId + "_" + counter;
        String path = JobManagerConfiguration.DEFAULT_REPOSITORY_PATH + "/assigned/" + localSlingId + "/" + topic + "/" + jobId;
        
        final UnDeserializableDataObject uddao = new UnDeserializableDataObject();
        return contentBuilder.resource(path, 
                ResourceHelper.PROPERTY_JOB_TOPIC, topic, 
                ResourceHelper.PROPERTY_JOB_ID, jobId,
                Job.PROPERTY_JOB_CREATED, Calendar.getInstance(),
                "uddao", uddao);
    }

    private static final class UnDeserializableDataObject implements Externalizable {
        private static final long serialVersionUID = 1L;

        public UnDeserializableDataObject() {
            // we'll allow this one
        }
        
        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.write(42);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cnfeCount.incrementAndGet();
            throw new ClassNotFoundException("UnDeserializableDataObject");
        }
    }
}
