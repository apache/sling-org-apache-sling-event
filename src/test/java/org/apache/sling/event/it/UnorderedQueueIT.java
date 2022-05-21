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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sling.event.impl.Barrier;
import org.apache.sling.event.impl.jobs.config.ConfigurationConstants;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.NotificationConstants;
import org.apache.sling.event.jobs.Queue;
import org.apache.sling.event.jobs.QueueConfiguration;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.factoryConfiguration;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class UnorderedQueueIT extends AbstractJobHandlingIT {

    private static final String QUEUE_NAME = "unorderedtestqueue";
    private static final String TOPIC = "sling/unorderedtest";
    private static int MAX_PAR = 5;
    private static int NUM_JOBS = 300;

    @Configuration
    public Option[] configuration() {
        return options(
            baseConfiguration(),
            // create round robin test queue
            factoryConfiguration("org.apache.sling.event.jobs.QueueConfiguration")
                .put(ConfigurationConstants.PROP_NAME, QUEUE_NAME)
                .put(ConfigurationConstants.PROP_TYPE, QueueConfiguration.Type.UNORDERED.name())
                .put(ConfigurationConstants.PROP_TOPICS, TOPIC + "/*")
                .put(ConfigurationConstants.PROP_RETRIES, 2)
                .put(ConfigurationConstants.PROP_RETRY_DELAY, 2000L)
                .put(ConfigurationConstants.PROP_MAX_PARALLEL, MAX_PAR)
                .asOption()
        );
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testUnorderedQueue() throws Exception {

        final Barrier cb = new Barrier(2);

        this.registerJobConsumer(TOPIC + "/start",
                new JobConsumer() {

                    @Override
                    public JobResult process(final Job job) {
                        cb.block();
                        return JobResult.OK;
                    }
                });

        // register new consumer and event handle
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicInteger parallelCount = new AtomicInteger(0);
        final Set<Integer> maxParticipants = new HashSet<Integer>();

        this.registerJobConsumer(TOPIC + "/*",
                new JobConsumer() {

                    @Override
                    public JobResult process(final Job job) {
                        final int max = parallelCount.incrementAndGet();
                        if ( max > MAX_PAR ) {
                            parallelCount.decrementAndGet();
                            return JobResult.FAILED;
                        }
                        synchronized ( maxParticipants ) {
                            maxParticipants.add(max);
                        }
                        sleep(job.getProperty("sleep", 30));
                        parallelCount.decrementAndGet();
                        return JobResult.OK;
                    }
                });
        this.registerEventHandler(NotificationConstants.TOPIC_JOB_FINISHED,
                new EventHandler() {

                    @Override
                    public void handleEvent(final Event event) {
                        count.incrementAndGet();
                    }
                });

        // we first sent one event to get the queue started
        jobManager.addJob(TOPIC + "/start", null);
        assertTrue("No event received in the given time.", cb.block(5));
        cb.reset();

        // get the queue
        final Queue q = jobManager.getQueue(QUEUE_NAME);
        assertNotNull("Queue '" + QUEUE_NAME + "' should exist!", q);

        // suspend it
        q.suspend();

        // we start "some" jobs:
        for(int i = 0; i < NUM_JOBS; i++ ) {
            final String subTopic = TOPIC + "/sub" + (i % 10);
            final Map<String, Object> props = new HashMap<String, Object>();
            if ( i < 10 ) {
                props.put("sleep", 300);
            } else {
                props.put("sleep", 30);
            }
            jobManager.addJob(subTopic, props);
        }
        // start the queue
        q.resume();
        while ( count.get() < NUM_JOBS  + 1 ) {
            assertEquals("Failed count", 0, q.getStatistics().getNumberOfFailedJobs());
            assertEquals("Cancelled count", 0, q.getStatistics().getNumberOfCancelledJobs());
            sleep(300);
        }
        // we started one event before the test, so add one
        assertEquals("Finished count", NUM_JOBS + 1, count.get());
        assertEquals("Finished count", NUM_JOBS + 1, jobManager.getStatistics().getNumberOfFinishedJobs());
        assertEquals("Finished count", NUM_JOBS + 1, q.getStatistics().getNumberOfFinishedJobs());
        assertEquals("Failed count", 0, q.getStatistics().getNumberOfFailedJobs());
        assertEquals("Cancelled count", 0, q.getStatistics().getNumberOfCancelledJobs());
        for(int i=1; i <= MAX_PAR; i++) {
            assertTrue("# Participants " + String.valueOf(i) + " not in " + maxParticipants,
                    maxParticipants.contains(i));
        }
    }
}
