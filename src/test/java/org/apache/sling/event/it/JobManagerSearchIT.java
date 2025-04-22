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

import static org.ops4j.pax.exam.CoreOptions.options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.JobManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class JobManagerSearchIT extends AbstractJobHandlingIT {

    public static final String TOPIC = "sling/test";

    private final ArrayList<String> registeredJobIds = new ArrayList<>();

    @Configuration
    public Option[] configuration() {
        return options(
            baseConfiguration()
        );
    }

    @Override
    @Before
    public void setup() throws IOException {
        super.setup();
        JobManager jobManager = this.getJobManager();

        registeredJobIds.add(jobManager.addJob(TOPIC, Collections.<String, Object>emptyMap()).getId());
        registeredJobIds.add(jobManager.addJob(TOPIC, Collections.<String, Object>singletonMap("filter", "a")).getId());
        Map<String, Object> properties = new HashMap<String, Object>(2);
        properties.put("filter", "a");
        properties.put("property", "b");
        registeredJobIds.add(jobManager.addJob(TOPIC, properties).getId());
        registeredJobIds.add(jobManager.addJob(TOPIC, Collections.<String, Object>singletonMap("filter", "c")).getId());

        this.sleep(2000L);
    }

    private JobManager getJobManager() {
        return jobManager;
    }

    @After
    public void cleanup() {
        JobManager jobManager = getJobManager();
        for (String id : registeredJobIds) {
            jobManager.removeJobById(id);
        }
        registeredJobIds.clear();
    }

    @Test
    public void testJobSearchAll() {
        Collection<Job> res = this.getJobManager().findJobs(JobManager.QueryType.ALL, TOPIC, 0);
        Assert.assertEquals("Search did not return right amount of hits", registeredJobIds.size(), res.size());
        for (Job job : res) {
            Assert.assertTrue("Searched returned unregistered Job", registeredJobIds.contains(job.getId()));
        }
    }

    @Test
    public void testJobSearchTemplateEquals() {
        Map<String, Object> template = Collections.<String, Object>singletonMap("=filter", "a");
        Collection<Job> res = this.getJobManager().findJobs(JobManager.QueryType.ALL, TOPIC, 0, template);
        checkResult(res, registeredJobIds.get(1), registeredJobIds.get(2));
    }
    @Test
    public void testJobSearchTemplateEqualsLarger() {
        Map<String, Object> template = Collections.<String, Object>singletonMap("<=property", "b");
        Collection<Job> res = this.getJobManager().findJobs(JobManager.QueryType.ALL, TOPIC, 0, template);
        checkResult(res, registeredJobIds.get(2));
    }

    @Test
    public void testJobSearchTemplateAnd() {
        Map<String, Object> template = new HashMap<>(2);
        template.put("=filter", "a");
        template.put("=property", "b");
        Collection<Job> res = this.getJobManager().findJobs(JobManager.QueryType.ALL, TOPIC, 0, template);
        checkResult(res, registeredJobIds.get(2));
    }

    @Test
    public void testJobSearchTemplateOr() {
        Map<String, Object> template1 = Collections.<String, Object>singletonMap("=filter", "a");
        Map<String, Object> template2 = Collections.<String, Object>singletonMap("=filter", "c");
        Collection<Job> res = this.getJobManager().findJobs(JobManager.QueryType.ALL, TOPIC, 0, template1, template2);
        checkResult(res, registeredJobIds.get(1), registeredJobIds.get(2), registeredJobIds.get(3));
    }

    private void checkResult(Collection<Job> result, String... expectedIds) {
        Assert.assertEquals("Search did not return right amount of hits", expectedIds.length, result.size());
        List<String> resultIds = Arrays.asList(expectedIds);
        for (Job job : result) {
            Assert.assertTrue("Search did not find expected Job", resultIds.contains(job.getId()));
        }
    }
}
