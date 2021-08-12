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
package org.apache.sling.event.it;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyEvent.Type;
import org.apache.sling.discovery.TopologyEventListener;
import org.apache.sling.discovery.TopologyView;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.event.jobs.NotificationConstants;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMaxParallelTest extends AbstractJobHandlingTest {

    private static final int BACKGROUND_LOAD_DELAY_SECONDS = 1;

    private static final int EXTRA_CHAOS_DURATION_SECONDS = 20;

    private static final int UNKNOWN_TOPOLOGY_FACTOR_MILLIS = 15;//100;

    private static final int STABLE_TOPOLOGY_FACTOR_MILLIS = 40;//300;

    static final String TOPIC_PREFIX = "sling/maxparallel/";

    static final String TOPIC_NAME = TOPIC_PREFIX + "zero";

    private final Object syncObj = new Object();

    protected int max = -1;

    @Override
    protected long backgroundLoadDelay() {
        return BACKGROUND_LOAD_DELAY_SECONDS;
    }

    private void registerMax(int cnt) {
        synchronized(syncObj) {
            max = Math.max(max, cnt);
        }
    }

    /**
     * Setup consumers
     */
    private void setupJobConsumers(long jobRunMillis) {
        this.registerJobConsumer(TOPIC_NAME,

            new JobConsumer() {

                private AtomicInteger cnt = new AtomicInteger(0);

                @Override
                public JobResult process(final Job job) {
                    int c = cnt.incrementAndGet();
                    registerMax(c);
                    log.info("process : start delaying. count=" + c + ", id="+ job.getId());
                    try {
                        Thread.sleep(jobRunMillis);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    log.info("process : done delaying. count=" + c + ", id="+ job.getId());
                    cnt.decrementAndGet();
                    return JobResult.OK;
                }
            });
    }

    private static final class CreateJobThread extends Thread {

        private final Logger log = LoggerFactory.getLogger(this.getClass());

        private final JobManager jobManager;

        final AtomicLong finishedThreads;

        private final Map<String, AtomicLong> created;

        private final int numJobs;

        public CreateJobThread(final JobManager jobManager,
                Map<String, AtomicLong> created,
                final AtomicLong finishedThreads,
                int numJobs) {
            this.jobManager = jobManager;
            this.created = created;
            this.finishedThreads = finishedThreads;
            this.numJobs = numJobs;
        }

        @Override
        public void run() {
            AtomicInteger cnt = new AtomicInteger(0);
            for(int i=0; i<numJobs; i++) {
                final int c = cnt.incrementAndGet();
                log.info("run: creating job " + c + " on topic " + TOPIC_NAME);
                if (jobManager.addJob(TOPIC_NAME, null) != null) {
                    created.get(TOPIC_NAME).incrementAndGet();
                }
            }
            finishedThreads.incrementAndGet();
        }

    }

    /**
     * Setup chaos thread(s)
     *
     * Chaos is right now created by sending topology changing/changed events randomly
     */
    private void setupChaosThreads(final List<Thread> threads,
            final AtomicLong finishedThreads, long duration) {
        final List<TopologyView> views = new ArrayList<>();
        // register topology listener
        final ServiceRegistration<TopologyEventListener> reg = this.bc.registerService(TopologyEventListener.class, new TopologyEventListener() {

            @Override
            public void handleTopologyEvent(final TopologyEvent event) {
                if ( event.getType() == Type.TOPOLOGY_INIT ) {
                    views.add(event.getNewView());
                }
            }
        }, null);
        while ( views.isEmpty() ) {
            this.sleep(10);
        }
        reg.unregister();
        final TopologyView view = views.get(0);

        try {
            final Collection<ServiceReference<TopologyEventListener>> refs = this.bc.getServiceReferences(TopologyEventListener.class, null);
            assertNotNull(refs);
            assertFalse(refs.isEmpty());
            TopologyEventListener found = null;
            for(final ServiceReference<TopologyEventListener> ref : refs) {
                final TopologyEventListener listener = this.bc.getService(ref);
                if ( listener != null && listener.getClass().getName().equals("org.apache.sling.event.impl.jobs.config.TopologyHandler") ) {
                    found = listener;
                    break;
                }
                bc.ungetService(ref);
            }
            assertNotNull(found);
            final TopologyEventListener tel = found;
            log.info("setupChaosThreads : simulating TOPOLOGY_INIT");
            tel.handleTopologyEvent(new TopologyEvent(Type.TOPOLOGY_INIT, null, view));

            threads.add(new Thread() {

                private final Random random = new Random();

                @Override
                public void run() {
                    final long startTime = System.currentTimeMillis();
                    // this thread runs 30 seconds longer than the job creation thread
                    final long endTime = startTime + (duration +EXTRA_CHAOS_DURATION_SECONDS) * 1000;
                    while ( System.currentTimeMillis() < endTime ) {
                        final int sleepTime = random.nextInt(25) + 15;
                        try {
                            Thread.sleep(sleepTime * STABLE_TOPOLOGY_FACTOR_MILLIS);
                        } catch ( final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        log.info("setupChaosThreads : simulating TOPOLOGY_CHANGING");
                        tel.handleTopologyEvent(new TopologyEvent(Type.TOPOLOGY_CHANGING, view, null));
                        final int changingTime = random.nextInt(20) + 3;
                        try {
                            Thread.sleep(changingTime * UNKNOWN_TOPOLOGY_FACTOR_MILLIS);
                        } catch ( final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        log.info("setupChaosThreads : simulating TOPOLOGY_CHANGED");
                        tel.handleTopologyEvent(new TopologyEvent(Type.TOPOLOGY_CHANGED, view, view));
                    }
                    tel.getClass().getName();
                    finishedThreads.incrementAndGet();
                }
            });
        } catch (InvalidSyntaxException e) {
            e.printStackTrace();
        }
    }

    void doTestMaxParallel(int numJobs, long jobRunMillis, long duration) throws Exception {

        final Map<String, AtomicLong> added = new HashMap<>();
        final Map<String, AtomicLong> created = new HashMap<>();
        final Map<String, AtomicLong> finished = new HashMap<>();
        final List<String> topics = new ArrayList<>();
        added.put(TOPIC_NAME, new AtomicLong());
        created.put(TOPIC_NAME, new AtomicLong());
        finished.put(TOPIC_NAME, new AtomicLong());
        topics.add(TOPIC_NAME);

        final List<Thread> threads = new ArrayList<>();
        final AtomicLong finishedThreads = new AtomicLong();

        this.registerEventHandler("org/apache/sling/event/notification/job/*",
                new EventHandler() {

                    @Override
                    public void handleEvent(final Event event) {
                        final String topic = (String) event.getProperty(NotificationConstants.NOTIFICATION_PROPERTY_JOB_TOPIC);
                        if ( NotificationConstants.TOPIC_JOB_FINISHED.equals(event.getTopic())) {
                            finished.get(topic).incrementAndGet();
                        } else if ( NotificationConstants.TOPIC_JOB_ADDED.equals(event.getTopic())) {
                            added.get(topic).incrementAndGet();
                        }
                    }
                });

        // setup job consumers
        this.setupJobConsumers(jobRunMillis);

        // setup job creation tests
        new CreateJobThread(jobManager, created, finishedThreads, numJobs).start();

        // wait until 1 job is being processed
        log.info("doTestMaxParallel : waiting until 1 job is being processed");
        while ( max <= 0 ) {
            this.sleep(100);
        }
        log.info("doTestMaxParallel : 1 job was processed, ready to go. max=" + max);

        this.setupChaosThreads(threads, finishedThreads, duration);

        log.info("doTestMaxParallel : starting threads (" + threads.size() + ")");
        // start threads
        for(final Thread t : threads) {
            t.setDaemon(true);
            t.start();
        }

        log.info("doTestMaxParallel: sleeping for " + duration + " seconds to wait for threads to finish...");
        // for sure we can sleep for the duration
        this.sleep(duration * 1000);

        log.info("doTestMaxParallel: polling for threads to finish...");
        // wait until threads are finished
        while ( finishedThreads.get() < threads.size() ) {
            this.sleep(100);
        }

        final Set<String> allTopics = new HashSet<>(topics);
        log.info("doTestMaxParallel: waiting for job handling to finish... " + allTopics.size());
        while ( !allTopics.isEmpty() ) {
            final Iterator<String> iter = allTopics.iterator();
            while ( iter.hasNext() ) {
                final String topic = iter.next();
                log.info("doTestMaxParallel: checking topic= " + topic +
                        ", finished=" + finished.get(topic).get() + ", created=" + created.get(topic).get());
                if ( finished.get(topic).get() == created.get(topic).get() ) {
                    iter.remove();
                }
            }
            log.info("doTestMaxParallel: waiting for job handling to finish... " + allTopics.size());
            this.sleep(1000);
        }
        log.info("doTestMaxParallel: done.");
    }
}
