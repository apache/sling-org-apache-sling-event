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

import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.ScheduledJobInfo;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.ops4j.pax.exam.CoreOptions.options;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class SchedulingIT extends AbstractJobHandlingIT {

    private static final String TOPIC = "job/scheduled/topic";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Configuration
    public Option[] configuration() {
        return options(baseConfiguration());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testScheduling() throws Exception {
        final AtomicInteger counter = new AtomicInteger();

        this.registerJobConsumer(TOPIC, new JobConsumer() {

            @Override
            public JobResult process(final Job job) {
                if (job.getTopic().equals(TOPIC)) {
                    counter.incrementAndGet();
                }
                return JobResult.OK;
            }
        });

        // we schedule three jobs
        final ScheduledJobInfo info1 =
                jobManager.createJob(TOPIC).schedule().hourly(5).add();
        assertNotNull(info1);
        final ScheduledJobInfo info2 =
                jobManager.createJob(TOPIC).schedule().daily(10, 5).add();
        assertNotNull(info2);
        final ScheduledJobInfo info3 =
                jobManager.createJob(TOPIC).schedule().weekly(3, 19, 12).add();
        assertNotNull(info3);
        // This is a duplicate and won't be scheduled, as it is identical to the 3rd job
        final ScheduledJobInfo info4 =
                jobManager.createJob(TOPIC).schedule().weekly(3, 19, 12).add();
        assertNotNull(info4);

        assertEquals(3, jobManager.getScheduledJobs().size()); // scheduled jobs
        info3.unschedule();
        assertEquals(2, jobManager.getScheduledJobs().size()); // scheduled jobs
        info1.unschedule();
        assertEquals(1, jobManager.getScheduledJobs().size()); // scheduled jobs
        info2.unschedule();
        assertEquals(0, jobManager.getScheduledJobs().size()); // scheduled jobs
    }

    @Test
    public void schedulingLoadTest() throws Exception {
        logger.info("schedulingLoadTest: start");
        final AtomicInteger counter = new AtomicInteger();
        final int NUM_ITERATIONS = 1500;
        final String ownTopic = "random/" + UUID.randomUUID().toString();
        this.registerJobConsumer(ownTopic, new JobConsumer() {

            @Override
            public JobResult process(final Job job) {
                if (job.getTopic().equals(ownTopic)) {
                    counter.incrementAndGet();
                }
                return JobResult.OK;
            }
        });
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            logger.info("schedulingLoadTest: loop-" + i);
            jobManager
                    .createJob(ownTopic)
                    .properties(Collections.singletonMap("prop", i))
                    .schedule()
                    .at(new Date(System.currentTimeMillis() + 2500))
                    .add();
            Thread.sleep(1);
        }
        logger.info(
                "schedulingLoadTest: done, letting jobs be triggered, currently at {} jobs, {} schedules",
                counter.get(),
                jobManager.getScheduledJobs().size());
        final long timeout = System.currentTimeMillis() + 60000;
        while (System.currentTimeMillis() < timeout) {
            if ((counter.get() == NUM_ITERATIONS)
                    && (jobManager.getScheduledJobs().size() == 0)) {
                break;
            }
            logger.info(
                    "schedulingLoadTest: currently at {} jobs, {} schedules",
                    counter.get(),
                    jobManager.getScheduledJobs().size());
            Thread.sleep(100);
        }
        assertEquals(NUM_ITERATIONS, counter.get());
        assertEquals(0, jobManager.getScheduledJobs().size());
        logger.info("schedulingLoadTest: end");
    }
}
