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
package org.apache.sling.event.impl.jobs.tasks;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.UUID;

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.discovery.ClusterView;
import org.apache.sling.discovery.InstanceDescription;
import org.apache.sling.discovery.TopologyView;
import org.apache.sling.discovery.base.commons.DefaultTopologyView;
import org.apache.sling.discovery.commons.providers.DefaultClusterView;
import org.apache.sling.discovery.commons.providers.DefaultInstanceDescription;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.config.TopologyCapabilities;
import org.apache.sling.event.impl.jobs.scheduling.JobSchedulerImpl;
import org.apache.sling.event.impl.support.ResourceHelper;
import org.apache.sling.testing.mock.sling.junit.SlingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class CleanUpTest {

    static class DynamicClock extends Clock {

        private final Clock baseClock;
        private Duration offset;

        DynamicClock(Clock baseClock, Duration offsetOrNull) {
            this.baseClock = baseClock;
            setOffset(offsetOrNull);
        }

        public void setOffset(Duration offsetOrNull) {
            this.offset = offsetOrNull;
        }

        public void incrementOffset(Duration offsetOrNull) {
            if (offsetOrNull == null) {
                // then why call this?
                return;
            }
            if (this.offset == null) {
                this.offset = offsetOrNull;
            } else {
                final long newOffsetMillis = Math.addExact(offsetOrNull.toMillis(), offset.toMillis());
                this.offset = Duration.ofMillis(newOffsetMillis);
            }
        }

        @Override
        public ZoneId getZone() {
            return baseClock.getZone();
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return new DynamicClock(baseClock.withZone(zone), offset);
        }

        @Override
        public Instant instant() {
            if (offset == null) {
                return baseClock.instant();
            }
            return baseClock.instant().plus(offset);
        }

        @Override
        public long millis() {
            if (offset == null) {
                return baseClock.millis();
            }
            return Math.addExact(baseClock.millis(), offset.toMillis());
        }
    }

    private static final String JCR_PATH = JobManagerConfiguration.DEFAULT_REPOSITORY_PATH + "/cancelled";
    private static final String ASSIGNED_JOBS_JCR_PATH = JobManagerConfiguration.DEFAULT_REPOSITORY_PATH + "/assigned";
    private static final String UNASSIGNED_JOBS_JCR_PATH = JobManagerConfiguration.DEFAULT_REPOSITORY_PATH + "/unassigned";
    private static final String JCR_TOPIC = "test";
    private static final String JCR_JOB_NAME = "test-job";
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy/MM/dd/HH/mm");
//    private static final int MAX_AGE_IN_DAYS = 60;

    @Rule
    public final SlingContext ctx = new SlingContext();

    @Mock
    private JobManagerConfiguration configuration;
    @Mock
    private JobSchedulerImpl jobScheduler;

    @Rule
    public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.LENIENT);

    private final String localSlingId = "myLocalSlingId";
    private CleanUpTask task;
    private TopologyCapabilities capabilities;
    private TopologyView view;
    private DynamicClock clock;

    @Before
    public void setUp() {
        setupConfiguration();
        setUpTask();
        getCalendarInstance();
        createResource(JobManagerConfiguration.DEFAULT_REPOSITORY_PATH);

    }
    public static DefaultInstanceDescription createInstanceDescription(
            String instanceId, boolean isLocal, ClusterView clusterView) {
        if (!(clusterView instanceof DefaultClusterView)) {
            throw new IllegalArgumentException(
                    "Must pass a clusterView of type "
                            + DefaultClusterView.class);
        }
        DefaultInstanceDescription i = new DefaultInstanceDescription(
                (DefaultClusterView) clusterView, false, isLocal, instanceId, new HashMap<String, String>());
        return i;
    }

    public static DefaultTopologyView createTopologyView(String clusterViewId,
            String slingId) {
        DefaultTopologyView t = new DefaultTopologyView();
        DefaultClusterView c = new DefaultClusterView(clusterViewId);
        DefaultInstanceDescription i = new DefaultInstanceDescription(
                c, true, true, slingId, new HashMap<String, String>());
        Collection<InstanceDescription> instances = new LinkedList<InstanceDescription>();
        instances.add(i);
        t.addInstances(instances);
        return t;
    }

    private TopologyView createView(boolean current) {
        return createTopologyView(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    private void setupConfiguration() {
        Mockito.when(configuration.getLocalJobsPath()).thenReturn(ASSIGNED_JOBS_JCR_PATH + "/" + localSlingId);
        Mockito.when(configuration.getUnassignedJobsPath()).thenReturn(UNASSIGNED_JOBS_JCR_PATH);
        Mockito.when(configuration.getStoredCancelledJobsPath()).thenReturn(JCR_PATH);
        Mockito.when(configuration.createResourceResolver()).thenReturn(ctx.resourceResolver());
        Mockito.when(configuration.getHistoryCleanUpRemovedJobs()).thenReturn(1);

        view = createView(true);
        Mockito.when(configuration.getAssginedJobsPath()).thenReturn(ASSIGNED_JOBS_JCR_PATH);

        capabilities = new TopologyCapabilities(view, configuration);
        Mockito.when(configuration.getTopologyCapabilities()).thenReturn(capabilities);
        Mockito.when(capabilities.isActive()).thenReturn(true);
    }

    private void setUpTask() {
        task = new CleanUpTask(configuration, jobScheduler);
    }

//    private Resource createJobResourceForDate(Calendar cal, String status) {
//        String path = JCR_PATH + '/' + JCR_TOPIC + '/' + DATE_FORMATTER.format(cal.getTime()) + '/' + JCR_JOB_NAME;
//        return ctx.create().resource(path, JobImpl.PROPERTY_FINISHED_STATE, status);
//    }

    private Resource createJobResourceForDate(String rootPath, Calendar cal) {
        String path = rootPath + '/' + JCR_TOPIC + '/' + DATE_FORMATTER.format(cal.getTime()) + '/' + JCR_JOB_NAME;
        return ctx.create().resource(path, "sling:resourceType", ResourceHelper.RESOURCE_TYPE_JOB);
    }

//    private void deleteJobResourceForDate(String rootPath, Calendar cal) throws PersistenceException {
//        String path = rootPath + '/' + JCR_TOPIC + '/' + DATE_FORMATTER.format(cal.getTime()) + '/' + JCR_JOB_NAME;
//        ResourceResolver resolver = ctx.resourceResolver();
//        Resource r = resolver.getResource(path);
//        resolver.delete(r);
//        resolver.commit();
//    }

    private void deleteResource(Resource r) throws PersistenceException {
        ResourceResolver resolver = ctx.resourceResolver();
        resolver.delete(r);
        resolver.commit();
    }

    private Resource createEmptyJobResourceForDate(String rootPath, Calendar cal) {
        String path = rootPath + '/' + JCR_TOPIC + '/' + DATE_FORMATTER.format(cal.getTime());
        return ctx.create().resource(path);
    }

    private Resource createResource(String rootPath) {
        return ctx.create().resource(rootPath);
    }

    // new calendar based on current date
    private Calendar getCalendarInstance() {
        Calendar cal = Calendar.getInstance();
        return getCalendarInstance(cal.get(Calendar.YEAR),cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH),
                cal.get(Calendar.HOUR),cal.get(Calendar.MINUTE));
    }

    // new calendar based on provided data
    private Calendar getCalendarInstance(int year, int month, int day, int hour, int minute) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(year,month,day,hour,minute);
        this.clock = new DynamicClock(Clock.fixed(calendar.toInstant(), ZoneId.systemDefault()), null);
        task.setClock(clock);
        return calendar;
    }

    private void simulate(int num, Duration duration) {
        for(int i=0; i<num; i++) {
            clock.incrementOffset(duration);
            task.run();
        }
    }
    private int countFolders(String path) {
        final ResourceResolver resolver = this.configuration.createResourceResolver();
        if ( resolver == null ) {
            return -1;
        }
        try {
            final Resource baseResource = resolver.getResource(path);
            return countChildren(baseResource);
        } finally {
            resolver.close();
        }
    }
    private int countChildren(Resource parent) {
        if (parent == null) {
            return 0;
        }
        int count = 0;
        final Iterator<Resource> it = parent.listChildren();
        if (it != null) {
            while( it.hasNext() ) {
                final Resource child = it.next();
                final int childCount = countChildren(child);
                count += childCount;
            }
        }
        return 1 + count;
    }

    @Test
    public void testEmpty() {
        assertEquals(1, countFolders(JobManagerConfiguration.DEFAULT_REPOSITORY_PATH));
        createResource(ASSIGNED_JOBS_JCR_PATH);
        createResource(UNASSIGNED_JOBS_JCR_PATH);
        assertEquals(3, countFolders(JobManagerConfiguration.DEFAULT_REPOSITORY_PATH));
        // 2 days worth of cleanup
        for( int i = 0; i < 48; i++) {
            simulate(60, Duration.ofMinutes(1));
        }
        assertEquals(3, countFolders(JobManagerConfiguration.DEFAULT_REPOSITORY_PATH));
    }

    @Test
    public void testAssignedEmpty() {
        assertEquals(1, countFolders(JobManagerConfiguration.DEFAULT_REPOSITORY_PATH));
        createResource(ASSIGNED_JOBS_JCR_PATH);
        assertEquals(2, countFolders(JobManagerConfiguration.DEFAULT_REPOSITORY_PATH));
        for( int i = 0; i < 100; i++) {
            createResource(ASSIGNED_JOBS_JCR_PATH + "/" + UUID.randomUUID().toString());
        }
        assertEquals(102, countFolders(JobManagerConfiguration.DEFAULT_REPOSITORY_PATH));
        // 2 days worth of cleanup
        for( int i = 0; i < 480; i++) {
            simulate(60, Duration.ofMinutes(1));
        }
        assertEquals(2, countFolders(JobManagerConfiguration.DEFAULT_REPOSITORY_PATH));
    }

    @Test
    public void testAssignedSimple_nonLocal() throws PersistenceException {
        final String mySlingId = UUID.randomUUID().toString();
        Calendar calendar = getCalendarInstance();
        Resource job = createJobResourceForDate(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId, calendar);
        calendar.add(Calendar.DAY_OF_YEAR,-1);
        createEmptyJobResourceForDate(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId, calendar);
        assertEquals(11, countFolders(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId));

        // full clean up is done every hour
        // so let's simulate an hour
        simulate(60, Duration.ofMinutes(1));

        // after the first run it will just have marked the folder for deletion, but not deleted, so counter is still the same
        assertEquals(11, countFolders(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId));

        // simulate 72 hours
        for( int i = 0; i < 72; i++) {
            simulate(60, Duration.ofMinutes(1));
        }
        // nothing should be deleted
        assertEquals(11, countFolders(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId));

        deleteResource(job);
        for( int i = 0; i < 72; i++) {
            simulate(60, Duration.ofMinutes(1));
        }
        assertEquals(0, countFolders(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId));
    }

    @Test
    public void testAssignedSimple_local() throws PersistenceException {
        final String mySlingId = localSlingId;
        Calendar calendar = getCalendarInstance();
        Resource jobResource = createJobResourceForDate(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId, calendar);
        calendar.add(Calendar.DAY_OF_YEAR,-1);
        createEmptyJobResourceForDate(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId, calendar);
        assertEquals(11, countFolders(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId));

        // full clean up is done every hour
        // so let's simulate an hour
        simulate(60, Duration.ofMinutes(1));

        // after the first run it will just have marked the folder for deletion, but not deleted, so counter is still the same
        assertEquals(8, countFolders(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId));

        // simulate 24 hours - still nothing
        for( int i = 0; i < 24; i++) {
            simulate(60, Duration.ofMinutes(1));
            assertEquals("i = " + i, 8, countFolders(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId));
        }

        // lets delete the job
        deleteResource(jobResource);

        for( int i = 0; i < 25; i++) {
            simulate(60, Duration.ofMinutes(1));
        }
        assertEquals(0, countFolders(ASSIGNED_JOBS_JCR_PATH + "/" + mySlingId));
    }

    @Test
    public void testUnassigned_nothingToDelete() {
        Calendar calendar = getCalendarInstance();
        createJobResourceForDate(UNASSIGNED_JOBS_JCR_PATH, calendar);
        assertEquals(8, countFolders(UNASSIGNED_JOBS_JCR_PATH));

        // full clean up is done every hour
        simulate(60, Duration.ofMinutes(1));

        // fullEmptyFolderCleanup deletes only if 2 hour values older - so nothing changed yet
        assertEquals(8, countFolders(UNASSIGNED_JOBS_JCR_PATH));

        simulate(60, Duration.ofMinutes(1));
        assertEquals(8, countFolders(UNASSIGNED_JOBS_JCR_PATH));

        // simulate 23 hours
        for( int i = 0; i < 23; i++) {
            simulate(60, Duration.ofMinutes(1));
            assertEquals("i = " + i, 8, countFolders(UNASSIGNED_JOBS_JCR_PATH));
        }

        // now on the next run it should finally do its job
        simulate(60, Duration.ofMinutes(1));
        assertEquals(8, countFolders(UNASSIGNED_JOBS_JCR_PATH));
    }

    @Test
    public void testUnassignedSimple_deleteJob() throws PersistenceException {
        doTestUnassignedSimple(true, 4); // unassigned/test/2022/05
    }

    @Test
    public void testUnassignedSimple_noDeleteJob() throws PersistenceException {
        doTestUnassignedSimple(false, 8); // unassigned/test/2022/05/17/17/30/test-job
    }

    private void doTestUnassignedSimple(boolean deleteJob, int expectedFolders) throws PersistenceException {
        Calendar calendar = getCalendarInstance(2022,4,17,17,30); // 2022 May 17, 17:30
        Resource job = createJobResourceForDate(UNASSIGNED_JOBS_JCR_PATH, calendar);
        calendar.add(Calendar.DAY_OF_YEAR,-1);
        createEmptyJobResourceForDate(UNASSIGNED_JOBS_JCR_PATH, calendar);
        assertEquals(11, countFolders(UNASSIGNED_JOBS_JCR_PATH));

        // full clean up is done every hour
        // so let's simulate an hour
        simulate(60, Duration.ofMinutes(1));

        // after the first run it will just have marked the folder for deletion, but not deleted, 
        // so counter is still the same
        assertEquals(8, countFolders(UNASSIGNED_JOBS_JCR_PATH));

        if (deleteJob) deleteResource(job);

        // additional 72 hours
        for( int i = 0; i < 72; i++) {
            simulate(60, Duration.ofMinutes(1));
        }

        assertEquals(expectedFolders, countFolders(UNASSIGNED_JOBS_JCR_PATH));
    }

    @Test
    public void testUnassignedDec31() throws PersistenceException {
        Calendar calendar = getCalendarInstance(2021,11,31,17,30); // 2021 Dec 31, 17:30
        Resource job = createJobResourceForDate(UNASSIGNED_JOBS_JCR_PATH, calendar);
        calendar.add(Calendar.DAY_OF_YEAR,-1);
        createEmptyJobResourceForDate(UNASSIGNED_JOBS_JCR_PATH, calendar);
        assertEquals(11, countFolders(UNASSIGNED_JOBS_JCR_PATH));
        deleteResource(job);

        // 72 hours later => now it's 2022
        for( int i = 0; i < 72; i++) {
            simulate(60, Duration.ofMinutes(1));
        }
        assertEquals(2, countFolders(UNASSIGNED_JOBS_JCR_PATH)); // unassigned/test
    }
}
