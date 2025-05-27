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

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.sling.event.impl.jobs.JobImpl;
import org.apache.sling.event.impl.jobs.config.ConfigurationConstants;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.event.jobs.QueueConfiguration;
import org.apache.sling.event.jobs.consumer.JobExecutionContext;
import org.apache.sling.event.jobs.consumer.JobExecutionResult;
import org.apache.sling.event.jobs.consumer.JobExecutor;
import org.junit.Before;
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
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.factoryConfiguration;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class HistoryIT extends AbstractJobHandlingIT {

    private static final String TOPIC = "sling/test/history";

    private static final String PROP_COUNTER = "counter";

    @Configuration
    public Option[] configuration() {
        return options(
            baseConfiguration(),
            // create test queue - we use an ordered queue to have a stable processing order
            // keep the jobs in the history
            factoryConfiguration("org.apache.sling.event.jobs.QueueConfiguration")
                .put(ConfigurationConstants.PROP_NAME, "test")
                .put(ConfigurationConstants.PROP_TYPE, QueueConfiguration.Type.ORDERED.name())
                .put(ConfigurationConstants.PROP_TOPICS, new String[]{TOPIC})
                .put(ConfigurationConstants.PROP_RETRIES, 2)
                .put(ConfigurationConstants.PROP_RETRY_DELAY, 2L)
                .put(ConfigurationConstants.PROP_KEEP_JOBS, true)
                .asOption()
        );
    }

    private Job addJob(final long counter) {
        final Map<String, Object> props = new HashMap<String, Object>();
        props.put(PROP_COUNTER, counter);
        return jobManager.addJob(TOPIC, props );
    }

    @Before
    public void additionalStartupDelay() throws InterruptedException {
        Thread.sleep(2000);
    }

    /**
     * Test history.
     * Start 10 jobs and cancel some of them and succeed others
     */
    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testHistory() throws Exception {
        this.registerJobExecutor(TOPIC,
                new JobExecutor() {

                    @Override
                    public JobExecutionResult process(final Job job, final JobExecutionContext context) {
                        sleep(5L);
                        final long count = job.getProperty(PROP_COUNTER, Long.class);
                        if ( count == 2 || count == 5 || count == 7 ) {
                            return context.result().message(Job.JobState.ERROR.name()).cancelled();
                        }
                        return context.result().message(Job.JobState.SUCCEEDED.name()).succeeded();
                    }

                });

        OffsetDateTime start = OffsetDateTime.now();
        for(int i = 0; i< 10; i++) {
            this.addJob(i);
        }
        this.sleep(200L);
        while (jobManager.findJobs(JobManager.QueryType.HISTORY, TOPIC, -1, (Map<String, Object>[])null).size() < 10 ) {
            this.sleep(100L);
        }
        Collection<Job> col = jobManager.findJobs(JobManager.QueryType.HISTORY, TOPIC, -1, (Map<String, Object>[])null);
        assertEquals(10, col.size());
        assertEquals(0, jobManager.findJobs(JobManager.QueryType.ACTIVE, TOPIC, -1, (Map<String, Object>[])null).size());
        assertEquals(0, jobManager.findJobs(JobManager.QueryType.QUEUED, TOPIC, -1, (Map<String, Object>[])null).size());
        assertEquals(0, jobManager.findJobs(JobManager.QueryType.ALL, TOPIC, -1, (Map<String, Object>[])null).size());
        assertEquals(3, jobManager.findJobs(JobManager.QueryType.CANCELLED, TOPIC, -1, (Map<String, Object>[])null).size());
        assertEquals(0, jobManager.findJobs(JobManager.QueryType.DROPPED, TOPIC, -1, (Map<String, Object>[])null).size());
        assertEquals(3, jobManager.findJobs(JobManager.QueryType.ERROR, TOPIC, -1, (Map<String, Object>[])null).size());
        assertEquals(0, jobManager.findJobs(JobManager.QueryType.GIVEN_UP, TOPIC, -1, (Map<String, Object>[])null).size());
        assertEquals(0, jobManager.findJobs(JobManager.QueryType.STOPPED, TOPIC, -1, (Map<String, Object>[])null).size());
        assertEquals(7, jobManager.findJobs(JobManager.QueryType.SUCCEEDED, TOPIC, -1, (Map<String, Object>[])null).size());

        Map<String, Object> template = new HashMap<>();
        template.put(">=" + JobImpl.PROPERTY_JOB_STARTED_TIME, start.toString());
        template.put("<" + JobImpl.PROPERTY_FINISHED_DATE, OffsetDateTime.now().toString());
        assertEquals(7, jobManager.findJobs(JobManager.QueryType.SUCCEEDED, TOPIC, -1, template).size());

        // find all topics
        assertEquals(7, jobManager.findJobs(JobManager.QueryType.SUCCEEDED, null, -1, (Map<String, Object>[])null).size());

        // verify order, message and state
        long last = 9;
        for(final Job j : col) {
            assertNotNull(j.getFinishedDate());
            final long count = j.getProperty(PROP_COUNTER, Long.class);
            assertEquals(last, count);
            if ( count == 2 || count == 5 || count == 7 ) {
                assertEquals(Job.JobState.ERROR, j.getJobState());
            } else {
                assertEquals(Job.JobState.SUCCEEDED, j.getJobState());
            }
            assertEquals(j.getJobState().name(), j.getResultMessage());
            last--;
        }
    }
}