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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.event.impl.TestUtil;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager.QueueInfo;
import org.apache.sling.event.impl.jobs.config.TopologyCapabilities;
import org.apache.sling.event.jobs.JobManager.QueryType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.osgi.service.condition.Condition;
import org.slf4j.Logger;

public class JobManagerImplTest {

    private static final String QUERY_ROOT = "/var/eventing/foobar";
    private static final QueryType QUERY_TYPE = QueryType.ACTIVE;

    private QueueInfo info;
    private TopologyCapabilities capabilities;
    private PrintStream originalErr;
    private ByteArrayOutputStream outContent;
    private JobManagerConfiguration configuration;

    private static final int LEVEL_DEBUG = 10;
    private static final int LEVEL_INFO = 20;
    private static final int LEVEL_WARN = 30;

    @Before
    public void init() throws IllegalAccessException {

        configuration = mock(JobManagerConfiguration.class);
        when(configuration.getUniquePath(any(), any(), any(), any())).thenReturn("/");
        when(configuration.getUniqueId(any())).thenReturn("1");
        when(configuration.getAuditLogger()).thenReturn(mock(Logger.class));

        QueueConfigurationManager queueConfigMgr = mock(QueueConfigurationManager.class);
        this.info = new QueueInfo();
        info.queueConfiguration = mock(InternalQueueConfiguration.class);
        when(queueConfigMgr.getQueueInfo(anyString())).thenReturn(info);
        when(configuration.getQueueConfigurationManager()).thenReturn(queueConfigMgr);

        this.capabilities = mock(TopologyCapabilities.class);
        when(configuration.getTopologyCapabilities()).thenReturn(capabilities);

        when(capabilities.detectTarget(eq("is/assigned"), any(), any())).thenReturn("assigned");

        ResourceResolver resolver = mock(ResourceResolver.class);
        when(resolver.getResource(anyString())).thenReturn(mock(Resource.class));
        when(configuration.createResourceResolver()).thenReturn(resolver);

        this.outContent = new ByteArrayOutputStream();
        this.originalErr = System.err;
        System.setErr(new PrintStream(outContent));

    }

    // SLING-8413
    @Test
    public void testTopicEscaping() {
        String baseQuery = JobManagerImpl.buildBaseQuery(QUERY_ROOT, "randomNonQuotedTopic", QUERY_TYPE, false);
        assertEquals(
                "/jcr:root/var/eventing/foobar/element(*,slingevent:Job)[@event.job.topic = "
                        + "'randomNonQuotedTopic' and not(@slingevent:finishedState) and @event.job.started.time",
                baseQuery);

        String baseQuery2 = JobManagerImpl.buildBaseQuery(QUERY_ROOT, "random'Topic", QUERY_TYPE, false);
        assertEquals("/jcr:root/var/eventing/foobar/element(*,slingevent:Job)[@event.job.topic = "
                + "'random''Topic' and not(@slingevent:finishedState) and @event.job.started.time", baseQuery2);

    }

    @Test
    public void wontLogAtWarn() throws IllegalAccessException, IOException, NoSuchMethodException, SecurityException,
            IllegalArgumentException, InvocationTargetException {
        JobManagerImpl jobManager = getJobManager(LEVEL_WARN);

        jobManager.addJob("not/assigned", null, null);
        assertFalse(TestUtil.containsLine(this.outContent, line -> line
                .contains("Persisting job Sling Job [topic=not/assigned] into queue null with no assigned target")));

        jobManager.addJob("is/assigned", null, null);
        assertFalse(TestUtil.containsLine(this.outContent, line -> line
                .contains("Persisting job Sling Job [topic=is/assigned] into queue null with no assigned target")));
    }

    @Test
    public void logsInfoForUnassigned() throws IllegalAccessException, IOException, NoSuchMethodException,
            SecurityException, IllegalArgumentException, InvocationTargetException {
        JobManagerImpl jobManager = getJobManager(LEVEL_INFO);
        jobManager.addJob("not/assigned", null, null);
        assertTrue(TestUtil.containsLine(this.outContent, line -> line
                .contains("Persisting job Sling Job [topic=not/assigned] into queue null with no assigned target")));

        jobManager.addJob("is/assigned", null, null);
        assertTrue(TestUtil.containsLine(this.outContent, line -> !line
                .contains("Persisting job Sling Job [topic=is/assigned] into queue null with no assigned target")));
    }

    @Test
    public void logsDebugForAssigned() throws IllegalAccessException, IOException, NoSuchMethodException,
            SecurityException, IllegalArgumentException, InvocationTargetException {
        JobManagerImpl jobManager = getJobManager(LEVEL_DEBUG);

        jobManager.addJob("is/assigned", null, null);
        assertTrue(TestUtil.containsLine(this.outContent, line -> line
                .contains("Persisting job Sling Job [topic=is/assigned] into queue null, target=assigned")));
    }

    @After
    public void after() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        System.setErr(originalErr);
    }

    private JobManagerImpl getJobManager(int level) throws IllegalAccessException, NoSuchMethodException,
            SecurityException, IllegalArgumentException, InvocationTargetException {

        JobManagerImpl jobManager = new JobManagerImpl();

        Object logger = FieldUtils.readDeclaredField(jobManager, "logger", true);
        FieldUtils.writeDeclaredField(logger, "currentLogLevel", level, true);
        Condition condition = mock(Condition.class);
        FieldUtils.writeDeclaredField(jobManager, "condition", condition, true);

        FieldUtils.writeDeclaredField(jobManager, "configuration", configuration, true);
        return jobManager;
    }

}
