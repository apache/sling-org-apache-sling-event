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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyEvent.Type;
import org.apache.sling.discovery.TopologyEventListener;
import org.apache.sling.discovery.TopologyView;
import org.apache.sling.event.impl.jobs.config.ConfigurationConstants;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.event.jobs.NotificationConstants;
import org.apache.sling.event.jobs.QueueConfiguration;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.factoryConfiguration;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class ChaosIT extends AbstractJobHandlingIT {

    /** Duration for firing jobs in seconds. */
    private static final long DURATION = 1 * 60;

    /** Grace period (in seconds) the chaos thread keeps running after job creation has finished. */
    private static final int CHAOS_GRACE_SECONDS = 5;

    /** Maximum time (in seconds) to wait for all created jobs to finish processing. */
    private static final int JOB_DRAIN_TIMEOUT_SECONDS = 600;

    private static final int NUM_ORDERED_THREADS = 3;
    private static final int NUM_PARALLEL_THREADS = 6;
    private static final int NUM_ROUND_THREADS = 6;

    private static final int NUM_ORDERED_TOPICS = 2;
    private static final int NUM_PARALLEL_TOPICS = 8;
    private static final int NUM_ROUND_TOPICS = 8;

    private static final String ORDERED_TOPIC_PREFIX = "sling/chaos/ordered/";
    private static final String PARALLEL_TOPIC_PREFIX = "sling/chaos/parallel/";
    private static final String ROUND_TOPIC_PREFIX = "sling/chaos/round/";

    private static final String[] ORDERED_TOPICS = new String[NUM_ORDERED_TOPICS];
    private static final String[] PARALLEL_TOPICS = new String[NUM_PARALLEL_TOPICS];
    private static final String[] ROUND_TOPICS = new String[NUM_ROUND_TOPICS];

    static {
        for (int i = 0; i < NUM_ORDERED_TOPICS; i++) {
            ORDERED_TOPICS[i] = ORDERED_TOPIC_PREFIX + String.valueOf(i);
        }
        for (int i = 0; i < NUM_PARALLEL_TOPICS; i++) {
            PARALLEL_TOPICS[i] = PARALLEL_TOPIC_PREFIX + String.valueOf(i);
        }
        for (int i = 0; i < NUM_ROUND_TOPICS; i++) {
            ROUND_TOPICS[i] = ROUND_TOPIC_PREFIX + String.valueOf(i);
        }
    }

    @Configuration
    public Option[] configuration() {
        return options(
                baseConfiguration(),
                // create ordered test queue
                factoryConfiguration("org.apache.sling.event.jobs.QueueConfiguration")
                        .put(ConfigurationConstants.PROP_NAME, "chaos-ordered")
                        .put(ConfigurationConstants.PROP_TYPE, QueueConfiguration.Type.ORDERED.name())
                        .put(ConfigurationConstants.PROP_TOPICS, ORDERED_TOPICS)
                        .put(ConfigurationConstants.PROP_RETRIES, 2)
                        .put(ConfigurationConstants.PROP_RETRY_DELAY, 2000L)
                        .asOption(),
                // create round robin test queue
                factoryConfiguration("org.apache.sling.event.jobs.QueueConfiguration")
                        .put(ConfigurationConstants.PROP_NAME, "chaos-roundrobin")
                        .put(ConfigurationConstants.PROP_TYPE, QueueConfiguration.Type.TOPIC_ROUND_ROBIN.name())
                        .put(ConfigurationConstants.PROP_TOPICS, ROUND_TOPICS)
                        .put(ConfigurationConstants.PROP_RETRIES, 2)
                        .put(ConfigurationConstants.PROP_RETRY_DELAY, 2000L)
                        .put(ConfigurationConstants.PROP_MAX_PARALLEL, 5)
                        .asOption());
    }

    /**
     * Setup consumers
     */
    private void setupJobConsumers() {
        for (int i = 0; i < NUM_ORDERED_TOPICS; i++) {
            this.registerJobConsumer(ORDERED_TOPICS[i], new JobConsumer() {

                @Override
                public JobResult process(final Job job) {
                    return JobResult.OK;
                }
            });
        }
        for (int i = 0; i < NUM_PARALLEL_TOPICS; i++) {
            this.registerJobConsumer(PARALLEL_TOPICS[i], new JobConsumer() {

                @Override
                public JobResult process(final Job job) {
                    return JobResult.OK;
                }
            });
        }
        for (int i = 0; i < NUM_ROUND_TOPICS; i++) {
            this.registerJobConsumer(ROUND_TOPICS[i], new JobConsumer() {

                @Override
                public JobResult process(final Job job) {
                    return JobResult.OK;
                }
            });
        }
    }

    private static final class CreateJobThread extends Thread {

        private final String[] topics;

        private final JobManager jobManager;

        private final Random random = new Random();

        final Map<String, AtomicLong> created;

        final CountDownLatch creationLatch;

        final CountDownLatch allThreadsLatch;

        public CreateJobThread(
                final JobManager jobManager,
                final String[] topics,
                final Map<String, AtomicLong> created,
                final CountDownLatch creationLatch,
                final CountDownLatch allThreadsLatch) {
            this.topics = topics;
            this.jobManager = jobManager;
            this.created = created;
            this.creationLatch = creationLatch;
            this.allThreadsLatch = allThreadsLatch;
        }

        @Override
        public void run() {
            int index = 0;
            final long startTime = System.currentTimeMillis();
            final long endTime = startTime + DURATION * 1000;
            while (System.currentTimeMillis() < endTime) {
                final String topic = topics[index];
                if (jobManager.addJob(topic, null) != null) {
                    created.get(topic).incrementAndGet();

                    index++;
                    if (index == topics.length) {
                        index = 0;
                    }
                }
                final int sleepTime = random.nextInt(200);
                try {
                    this.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            creationLatch.countDown();
            allThreadsLatch.countDown();
        }
    }

    /**
     * Setup job creation threads
     */
    private void setupJobCreationThreads(
            final List<Thread> threads,
            final JobManager jobManager,
            final Map<String, AtomicLong> created,
            final CountDownLatch creationLatch,
            final CountDownLatch allThreadsLatch) {
        for (int i = 0; i < NUM_ORDERED_THREADS; i++) {
            threads.add(new CreateJobThread(jobManager, ORDERED_TOPICS, created, creationLatch, allThreadsLatch));
        }
        for (int i = 0; i < NUM_PARALLEL_THREADS; i++) {
            threads.add(new CreateJobThread(jobManager, PARALLEL_TOPICS, created, creationLatch, allThreadsLatch));
        }
        for (int i = 0; i < NUM_ROUND_THREADS; i++) {
            threads.add(new CreateJobThread(jobManager, ROUND_TOPICS, created, creationLatch, allThreadsLatch));
        }
    }

    /**
     * Setup chaos thread(s)
     *
     * Chaos is right now created by sending topology changing/changed events randomly
     */
    private void setupChaosThreads(
            final List<Thread> threads, final CountDownLatch creationLatch, final CountDownLatch allThreadsLatch) {
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

            threads.add(new Thread("chaos-topology-change") {

                private final Random random = new Random();

                @Override
                public void run() {
                    // keep creating chaos while jobs are being created and for a short grace
                    // period afterwards, so the drain phase is exercised under topology changes too
                    long graceDeadline = -1;
                    while (true) {
                        if (creationLatch.getCount() == 0) {
                            if (graceDeadline < 0) {
                                graceDeadline = System.currentTimeMillis() + CHAOS_GRACE_SECONDS * 1000L;
                            } else if (System.currentTimeMillis() >= graceDeadline) {
                                break;
                            }
                        }
                        final int sleepTime = random.nextInt(25) + 15;
                        chaosSleep(sleepTime * 1000L);
                        tel.handleTopologyEvent(new TopologyEvent(Type.TOPOLOGY_CHANGING, view, null));
                        log.info("Sent TopologyEvent (newView = null)");
                        final int changingTime = random.nextInt(20) + 3;
                        chaosSleep(changingTime * 1000L);
                        tel.handleTopologyEvent(new TopologyEvent(Type.TOPOLOGY_CHANGED, view, view));
                        log.info("Sent TopologyEvent (newView not null)");
                    }
                    allThreadsLatch.countDown();
                }

                /**
                 * Sleep up to {@code totalMillis}, but in small chunks so the stop condition is
                 * re-checked frequently. Returns early once job creation has finished, so the
                 * grace period is actually honoured instead of being masked by a single long
                 * sleep. A thread interrupt also terminates the sleep instead of busy-looping.
                 */
                private void chaosSleep(final long totalMillis) {
                    long remaining = totalMillis;
                    while (remaining > 0) {
                        final long chunk = Math.min(remaining, 250L);
                        try {
                            Thread.sleep(chunk);
                        } catch (final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        remaining -= chunk;
                        if (creationLatch.getCount() == 0) {
                            return;
                        }
                    }
                }
            });
        } catch (InvalidSyntaxException e) {
            e.printStackTrace();
        }
    }

    @Test(timeout = DURATION * 16000L)
    public void testDoChaos() throws Exception {

        // setup added, created and finished map
        // added and finished are filled by notifications
        // created is filled by the threads starting jobs
        final Map<String, AtomicLong> added = new HashMap<>();
        final Map<String, AtomicLong> created = new HashMap<>();
        final Map<String, AtomicLong> finished = new HashMap<>();
        final List<String> topics = new ArrayList<>();
        for (int i = 0; i < NUM_ORDERED_TOPICS; i++) {
            added.put(ORDERED_TOPICS[i], new AtomicLong());
            created.put(ORDERED_TOPICS[i], new AtomicLong());
            finished.put(ORDERED_TOPICS[i], new AtomicLong());
            topics.add(ORDERED_TOPICS[i]);
        }
        for (int i = 0; i < NUM_PARALLEL_TOPICS; i++) {
            added.put(PARALLEL_TOPICS[i], new AtomicLong());
            created.put(PARALLEL_TOPICS[i], new AtomicLong());
            finished.put(PARALLEL_TOPICS[i], new AtomicLong());
            topics.add(PARALLEL_TOPICS[i]);
        }
        for (int i = 0; i < NUM_ROUND_TOPICS; i++) {
            added.put(ROUND_TOPICS[i], new AtomicLong());
            created.put(ROUND_TOPICS[i], new AtomicLong());
            finished.put(ROUND_TOPICS[i], new AtomicLong());
            topics.add(ROUND_TOPICS[i]);
        }

        final List<Thread> threads = new ArrayList<>();
        final int numCreationThreads = NUM_ORDERED_THREADS + NUM_PARALLEL_THREADS + NUM_ROUND_THREADS;
        // Signals that all job-creation threads have finished. This is the chaos thread's only cue
        // to wind down (arm its grace period and stop). It cannot key off allThreadsLatch for this,
        // since that latch includes the chaos thread itself and can never reach zero from within it.
        final CountDownLatch creationLatch = new CountDownLatch(numCreationThreads);
        // all creation threads plus the single chaos thread; awaited by the test to know when every
        // worker thread has terminated
        final CountDownLatch allThreadsLatch = new CountDownLatch(numCreationThreads + 1);

        this.registerEventHandler("org/apache/sling/event/notification/job/*", new EventHandler() {

            @Override
            public void handleEvent(final Event event) {
                final String topic = (String) event.getProperty(NotificationConstants.NOTIFICATION_PROPERTY_JOB_TOPIC);
                if (NotificationConstants.TOPIC_JOB_FINISHED.equals(event.getTopic())) {
                    finished.get(topic).incrementAndGet();
                } else if (NotificationConstants.TOPIC_JOB_ADDED.equals(event.getTopic())) {
                    added.get(topic).incrementAndGet();
                }
            }
        });

        // setup job consumers
        this.setupJobConsumers();

        // setup job creation tests
        this.setupJobCreationThreads(threads, jobManager, created, creationLatch, allThreadsLatch);

        this.setupChaosThreads(threads, creationLatch, allThreadsLatch);

        log.info("Starting threads...");
        // start threads
        for (final Thread t : threads) {
            t.setDaemon(true);
            t.start();
        }

        log.info("Waiting for threads to finish...");
        // wait until all threads (job creation + chaos) have finished; the creation threads run
        // for DURATION seconds, the chaos thread for a short grace period longer
        final long threadTimeout = (DURATION + CHAOS_GRACE_SECONDS) * 1000L + 60_000L;
        boolean allThreadsFinished = allThreadsLatch.await(threadTimeout, TimeUnit.MILLISECONDS);

        // there is a small race condition in here, .getCount() could be zero already now
        assertTrue(
                "Job creation and chaos threads did not finish in time, still waiting for " + allThreadsLatch.getCount()
                        + "threads",
                allThreadsFinished);

        log.info("Waiting for job handling to finish...");
        final Set<String> allTopics = new HashSet<>(topics);
        final long drainDeadline = System.currentTimeMillis() + JOB_DRAIN_TIMEOUT_SECONDS * 1000L;
        long lastProgressLog = 0;
        while (!allTopics.isEmpty()) {
            final Iterator<String> iter = allTopics.iterator();
            while (iter.hasNext()) {
                final String topic = iter.next();
                if (finished.get(topic).get() == created.get(topic).get()) {
                    iter.remove();
                }
            }
            if (!allTopics.isEmpty()) {
                final long now = System.currentTimeMillis();
                // log progress every 5 seconds so a genuine stall can be told apart from slow draining
                if (now - lastProgressLog >= 5000) {
                    lastProgressLog = now;
                    long remaining = 0;
                    for (final String topic : allTopics) {
                        remaining +=
                                created.get(topic).get() - finished.get(topic).get();
                    }
                    log.info(
                            "Still draining: {} topics pending, {} jobs outstanding (created vs finished)",
                            allTopics.size(),
                            remaining);
                }
                assertTrue(
                        "Jobs did not finish within " + JOB_DRAIN_TIMEOUT_SECONDS + " seconds; topics still pending: "
                                + allTopics,
                        now < drainDeadline);
                this.sleep(100);
            }
        }
        log.info("Completed");
        /* We could try to enable this with Oak again - but right now JR observation handler is too
        * slow.
                   System.out.println("Checking notifications...");
                   for(final String topic : topics) {
                       assertEquals("Checking topic " + topic, created.get(topic).get(), added.get(topic).get());
                   }
        */

    }
}
