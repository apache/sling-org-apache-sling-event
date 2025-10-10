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

import java.util.Date;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.ops4j.pax.exam.CoreOptions.options;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class TimedJobsIT extends AbstractJobHandlingIT {

    private static final String TOPIC = "timed/test/topic";

    @Configuration
    public Option[] configuration() {
        return options(baseConfiguration());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testTimedJob() throws Exception {
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

        final Date d = new Date();
        d.setTime(System.currentTimeMillis() + 3000); // run in 3 seconds

        // create scheduled job
        final ScheduledJobInfo info =
                jobManager.createJob(TOPIC).schedule().at(d).add();
        assertNotNull(info);

        while (counter.get() == 0) {
            this.sleep(1000);
        }
        assertEquals(0, jobManager.getScheduledJobs().size()); // job is not scheduled anymore
        info.unschedule();
    }
}
