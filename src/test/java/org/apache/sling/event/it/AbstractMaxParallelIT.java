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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractMaxParallelIT extends AbstractJobHandlingIT {

    private static final int BACKGROUND_LOAD_DELAY_SECONDS = 1;

    /** Grace period (in seconds) the chaos thread keeps running after all jobs have finished. */
    private static final int CHAOS_GRACE_SECONDS = 5;

    private static final int UNKNOWN_TOPOLOGY_FACTOR_MILLIS = 15; // 100;

    private static final int STABLE_TOPOLOGY_FACTOR_MILLIS = 40; // 300;

    static final String TOPIC_PREFIX = "sling/maxparallel/";

    static final String TOPIC_NAME = TOPIC_PREFIX + "zero";

    private final Object syncObj = new Object();

    protected volatile int max = -1;

    @Override
    protected long backgroundLoadDelay() {
        return BACKGROUND_LOAD_DELAY_SECONDS;
    }

    private void registerMax(int cnt) {
        synchronized (syncObj) {
            max = Math.max(max, cnt);
        }
    }

    /**
     * Setup consumers
     */
    private void setupJobConsumers(long jobDuration) {
        this.registerJobConsumer(TOPIC_NAME, new JobConsumer() {

            private AtomicInteger concurrentExecutionsCounter = new AtomicInteger(0);

            @Override
            public JobResult process(final Job job) {
                int c = concurrentExecutionsCounter.incrementAndGet();
                registerMax(c);
                log.info("process : start delaying. concurrentExecutions={}, id={}", c, job.getId());
                try {
                    Thread.sleep(jobDuration);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info("process : done delaying. concurrentExecutions={}, id={}", c, job.getId());
                concurrentExecutionsCounter.decrementAndGet();
                return JobResult.OK;
            }
        });
    }

    private static final class CreateJobThread extends Thread {

        private final Logger log = LoggerFactory.getLogger(this.getClass());

        private final JobManager jobManager;

        private final Map<String, AtomicLong> created;

        private final int numJobs;

        public CreateJobThread(final JobManager jobManager, Map<String, AtomicLong> created, int numJobs) {
            this.jobManager = jobManager;
            this.created = created;
            this.numJobs = numJobs;
        }

        @Override
        public void run() {
            AtomicInteger cnt = new AtomicInteger(0);
            for (int i = 0; i < numJobs; i++) {
                final int c = cnt.incrementAndGet();
                log.info("run: creating job {} on topic {}", c, TOPIC_NAME);
                if (jobManager.addJob(TOPIC_NAME, null) != null) {
                    created.get(TOPIC_NAME).incrementAndGet();
                }
            }
        }
    }

    /**
     * Setup chaos thread(s)
     *
     * Chaos is right now created by sending topology changing/changed events randomly
     */
    private void setupChaosThreads(
            final List<Thread> threads, final CountDownLatch jobsLatch, final CountDownLatch chaosLatch) {
        final List<TopologyView> views = new ArrayList<>();
        // register topology listener
        final ServiceRegistration<TopologyEventListener> reg = this.bundleContext.registerService(
                TopologyEventListener.class,
                new TopologyEventListener() {

                    @Override
                    public void handleTopologyEvent(final TopologyEvent event) {
                        if (event.getType() == Type.TOPOLOGY_INIT) {
                            views.add(event.getNewView());
                        }
                    }
                },
                null);
        while (views.isEmpty()) {
            this.sleep(10);
        }
        reg.unregister();
        final TopologyView view = views.get(0);

        try {
            final Collection<ServiceReference<TopologyEventListener>> refs =
                    this.bundleContext.getServiceReferences(TopologyEventListener.class, null);
            assertNotNull(refs);
            assertFalse(refs.isEmpty());
            TopologyEventListener found = null;
            for (final ServiceReference<TopologyEventListener> ref : refs) {
                final TopologyEventListener listener = this.bundleContext.getService(ref);
                if (listener != null
                        && listener.getClass()
                                .getName()
                                .equals("org.apache.sling.event.impl.jobs.config.TopologyHandler")) {
                    found = listener;
                    break;
                }
                bundleContext.ungetService(ref);
            }
            assertNotNull(found);
            final TopologyEventListener tel = found;
            log.info("setupChaosThreads : simulating TOPOLOGY_INIT");
            tel.handleTopologyEvent(new TopologyEvent(Type.TOPOLOGY_INIT, null, view));

            threads.add(new Thread("topology-changer") {

                private final Random random = new Random();

                @Override
                public void run() {
                    long graceDeadline = -1;
                    while (true) {
                        if (jobsLatch.getCount() == 0) {
                            // keep creating chaos while jobs are still being processed and for a short
                            // grace period afterwards
                            if (graceDeadline < 0) {
                                graceDeadline = System.currentTimeMillis() + CHAOS_GRACE_SECONDS * 1000L;
                            } else if (System.currentTimeMillis() >= graceDeadline) {
                                break;
                            }
                        }
                        final int sleepTime = random.nextInt(25) + 15;
                        try {
                            Thread.sleep(sleepTime * STABLE_TOPOLOGY_FACTOR_MILLIS);
                        } catch (final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        log.info("setupChaosThreads : simulating TOPOLOGY_CHANGING");
                        tel.handleTopologyEvent(new TopologyEvent(Type.TOPOLOGY_CHANGING, view, null));
                        final int changingTime = random.nextInt(20) + 3;
                        try {
                            Thread.sleep(changingTime * UNKNOWN_TOPOLOGY_FACTOR_MILLIS);
                        } catch (final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        log.info("setupChaosThreads : simulating TOPOLOGY_CHANGED");
                        tel.handleTopologyEvent(new TopologyEvent(Type.TOPOLOGY_CHANGED, view, view));
                    }
                    chaosLatch.countDown();
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
        added.put(TOPIC_NAME, new AtomicLong());
        created.put(TOPIC_NAME, new AtomicLong());
        finished.put(TOPIC_NAME, new AtomicLong());

        final List<Thread> threads = new ArrayList<>();
        // count down for every job created
        final CountDownLatch jobsCreatedLatch = new CountDownLatch(numJobs);
        // counted down once for every finished job; lets us wait exactly until all jobs are done
        final CountDownLatch jobsCompletedLatch = new CountDownLatch(numJobs);
        // counted down by the chaos thread once it has stopped
        final CountDownLatch chaosLatch = new CountDownLatch(1);

        this.registerEventHandler("org/apache/sling/event/notification/job/*", new EventHandler() {

            @Override
            public void handleEvent(final Event event) {
                final String topic = (String) event.getProperty(NotificationConstants.NOTIFICATION_PROPERTY_JOB_TOPIC);
                if (NotificationConstants.TOPIC_JOB_FINISHED.equals(event.getTopic())) {
                    finished.get(topic).incrementAndGet();
                    jobsCompletedLatch.countDown();
                } else if (NotificationConstants.TOPIC_JOB_ADDED.equals(event.getTopic())) {
                    added.get(topic).incrementAndGet();
                    jobsCreatedLatch.countDown();
                }
            }
        });

        // setup job consumers
        this.setupJobConsumers(jobRunMillis);

        // setup job creation tests
        new CreateJobThread(jobManager, created, numJobs).start();

        // wait until 1 job is being processed
        log.info("doTestMaxParallel : waiting until at least 1 job is being processed");
        while (max <= 0) {
            this.sleep(100);
        }
        log.info("doTestMaxParallel : job processing started, ready to go");

        this.setupChaosThreads(threads, jobsCompletedLatch, chaosLatch);

        log.info("doTestMaxParallel : starting {} threads", threads.size());
        // start threads
        for (final Thread t : threads) {
            t.setDaemon(true);
            t.start();
        }

        // a generous timeout to wait until a jobs are created
        final long timeoutMillis = duration * 4 * 1000L;
        log.info("doTestMaxParallel: waiting for all {} jobs to be created...", numJobs);
        assertTrue(
                "Not all " + numJobs + " jobs created within " + (duration * 4) + " seconds",
                jobsCreatedLatch.await(timeoutMillis, TimeUnit.MILLISECONDS));
        log.info("All jobs were created");

        // generous timeout to fail fast instead of hanging if jobs get stuck; job processing under
        // topology chaos is much slower than the ideal throughput, so allow a wide margin while
        // still staying well below the JUnit @Test timeout
        log.info("doTestMaxParallel: waiting for all {} jobs to finish...", numJobs);
        assertTrue(
                "Not all " + numJobs + " jobs finished within " + (duration * 4) + " seconds",
                jobsCompletedLatch.await(timeoutMillis, TimeUnit.MILLISECONDS));

        log.info("doTestMaxParallel: waiting for chaos thread to stop...");
        assertTrue("Chaos thread did not stop in time", chaosLatch.await(CHAOS_GRACE_SECONDS * 2L, TimeUnit.SECONDS));

        log.info("doTestMaxParallel: done.");
    }
}
