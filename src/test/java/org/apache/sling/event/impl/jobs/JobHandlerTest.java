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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.event.impl.EnvironmentComponent;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager.QueueInfo;
import org.apache.sling.event.impl.jobs.config.TopologyCapabilities;
import org.apache.sling.event.impl.support.Environment;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.consumer.JobExecutor;
import org.apache.sling.serviceusermapping.ServiceUserMapped;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;


public class JobHandlerTest {
    
    public static final String JOB_TOPIC = "job/topic";
    public static final String JOB_ID = "12345";
    public static final String JOB_PATH = "/var/eventing/jobs/path/to/my/job";
    
    
    @Rule
    public SlingContext context = new SlingContext();
    
    
    ResourceResolver resolver; // a spy to the context.resourceResolver()
    
    JobManagerConfiguration config;
    JobImpl job;
    JobHandler handler;
    
    
    @Before
    public void setup() throws Exception {
        Environment.APPLICATION_ID = "slingId"; // oh, really hacky ...
        
        config = buildConfiguration();
        job = buildJobImpl();
        handler = new JobHandler (job, buildJobExecutor(), config);
    }
    
    
    @Test
    public void rescheduleWithNonExistingJobPath() throws Exception {
        
        // resolver.commit is used internally a few times, reset this spy before we actually start testing
        Mockito.reset(resolver); 
        assertFalse(handler.reschedule());
        Mockito.verify(resolver, Mockito.never()).commit();
    }
    
    @Test
    public void rescheduleWithExistingJobPath() throws Exception {
        context.build().resource(JOB_PATH, Collections.EMPTY_MAP).commit();
        // resolver.commit is used internally a few times, reset this spy before we actually start testing
        Mockito.reset(resolver); 
        assertTrue(handler.reschedule());
        Mockito.verify(resolver, Mockito.times(1)).commit();
    }
    
    @Test
    public void finishedKeepHistory () throws Exception {
        context.build().resource(JOB_PATH, Collections.EMPTY_MAP).commit();
        // resolver.commit is used internally a few times, reset this spy before we actually start testing
        Mockito.reset(resolver); 
        
        Job.JobState state = Job.JobState.SUCCEEDED;
        handler.finished(state,true, 1000L);
        
        // check that the jobpath has been deleted
        ArgumentCaptor<Resource> captor = ArgumentCaptor.forClass(Resource.class);
        Mockito.verify (resolver,Mockito.atLeast(1)).delete(captor.capture());
        assertEquals(JOB_PATH,captor.getValue().getPath());
        
        // check that the history resource is present
        String historyPath = config.getStoragePath(job.getTopic(), job.getId(), true);
        assertNotNull(resolver.getResource(historyPath)); 
    }
    
    @Test
    public void finishedDropHistory () throws Exception {
        context.build().resource(JOB_PATH, Collections.EMPTY_MAP).commit();
        // resolver.commit is used internally a few times, reset this spy before we actually start testing
        Mockito.reset(resolver); 
        
        Job.JobState state = Job.JobState.SUCCEEDED;
        
        handler.finished(state,false, 1000L);
        
        // check that the jobpath has been deleted
        ArgumentCaptor<Resource> captor = ArgumentCaptor.forClass(Resource.class);
        Mockito.verify (resolver,Mockito.atLeast(1)).delete(captor.capture());
        assertEquals(JOB_PATH,captor.getValue().getPath());
        
        // check that the history resource is not present
        String historyPath = config.getStoragePath(job.getTopic(), job.getId(), true);
        assertNull(resolver.getResource(historyPath)); 
    }
    
    
    @Test
    public void reassign () throws Exception {
        context.build().resource(JOB_PATH, Collections.EMPTY_MAP).commit();
        // resolver.commit is used internally a few times, reset this spy before we actually start testing
        Mockito.reset(resolver); 
        handler.reassign();
        
        // check that the old jobpath has been deleted
        ArgumentCaptor<Resource> captor = ArgumentCaptor.forClass(Resource.class);
        Mockito.verify (resolver,Mockito.atLeast(1)).delete(captor.capture());
        assertEquals(JOB_PATH,captor.getValue().getPath());
        
        // check that the reassigned job is present
        String newJobPath = config.getUniquePath("targetId", job.getTopic(), job.getId(), job.getProperties());
        assertNotNull(resolver.getResource(newJobPath)); 
    }
    
    @Test
    public void persistJobPropertiesTest() {
        
        Map<String,Object> props = new HashMap<>();
        props.put("prop1", "value1");
        props.put("toBeUpdated", "value2");
        props.put("toBeRemoved","value3");
        
        context.build().resource(JOB_PATH, props).commit();
        
        assertTrue(handler.persistJobProperties());
        assertTrue(handler.persistJobProperties(null));
        
        assertTrue(handler.persistJobProperties("toBeUpdated","toBeRemoved"));
        Resource jobResource = resolver.getResource(JOB_PATH);
        assertNotNull(jobResource);
        ValueMap vm = jobResource.adaptTo(ValueMap.class);
        assertNotNull(vm);
        
        assertEquals("value1",vm.get("prop1"));
        assertEquals("updatedValue2",vm.get("toBeUpdated"));
        assertNull(vm.get("toBeRemoved"));
        
        
    }
    
    
    // supporting methods
    
    private JobManagerConfiguration buildConfiguration () throws LoginException {
     
        JobManagerConfiguration originalConfiguration = new JobManagerConfiguration ();
        JobManagerConfiguration configuration = spy (originalConfiguration);
        ResourceResolverFactory rrf = context.getService(ResourceResolverFactory.class);
        
        // we don't care what type of resolver we use for these tests
        resolver = spy (rrf.getAdministrativeResourceResolver(null));
        
        when (configuration.createResourceResolver()).thenReturn(resolver);
        
        // just simple mocks for all dependencies for the JobManagerConfiguration and provide
        // stubs when required
        ServiceUserMapped sum = mock (ServiceUserMapped.class);
        context.registerService(ServiceUserMapped.class,sum);

        QueueConfigurationManager qcm = mock (QueueConfigurationManager.class);
        when (qcm.getQueueInfo(Mockito.anyString())).thenReturn(null);
        
        TopologyCapabilities caps = mock (TopologyCapabilities.class);
        when(caps.detectTarget(Mockito.matches(JOB_TOPIC), Matchers.<Map<String,Object>>any(), 
                Matchers.<QueueConfigurationManager.QueueInfo>any())).thenReturn("targetId");
        when(configuration.getTopologyCapabilities()).thenReturn(caps);
        
        context.registerService(qcm);
        
        EnvironmentComponent ec = mock (EnvironmentComponent.class);
        context.registerService(ec);

        // register this service and return
        context.registerInjectActivateService(configuration);
        return context.getService(JobManagerConfiguration.class);
        
    }
    
    private JobExecutor buildJobExecutor() {
        return mock (JobExecutor.class);
    }
    
    private JobImpl buildJobImpl() {
        
        Map<String,Object> props = new HashMap<>();
        props.put(JobImpl.PROPERTY_RESOURCE_PATH, JOB_PATH);
        props.put(Job.PROPERTY_RESULT_MESSAGE,"result message");
        props.put(Job.PROPERTY_JOB_STARTED_TIME, Calendar.getInstance());
        
        props.put("toBeUpdated", "updatedValue2");
        props.put("toBeRemoved",null);
        
        JobImpl job =  new JobImpl(JOB_TOPIC,JOB_ID, props);;
        
        return job;
    }
    
    

    
}
