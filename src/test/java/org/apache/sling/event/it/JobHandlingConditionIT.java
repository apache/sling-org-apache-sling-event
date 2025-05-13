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

import org.apache.sling.event.impl.jobs.config.ConfigurationConstants;
import org.apache.sling.event.jobs.QueueConfiguration;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.condition.Condition;

import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.factoryConfiguration;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class JobHandlingConditionIT extends AbstractJobHandlingIT {

    public static final String TOPIC = "sling/test/condition";

    private ServiceRegistration<Condition> jobProcessingConditionReg;

    @Configuration
    public Option[] configuration() {
        return options(
                baseConfiguration(),
                // create test queue
                factoryConfiguration("org.apache.sling.event.jobs.QueueConfiguration")
                        .put(ConfigurationConstants.PROP_NAME, "test")
                        .put(ConfigurationConstants.PROP_TYPE, QueueConfiguration.Type.UNORDERED.name())
                        .put(ConfigurationConstants.PROP_TOPICS, new String[]{TOPIC, TOPIC + "2"})
                        .put(ConfigurationConstants.PROP_RETRIES, 2)
                        .put(ConfigurationConstants.PROP_RETRY_DELAY, 2000L)
                        .asOption()
        );
    }

    @Before
    public void additionalStartupDelay() throws InterruptedException {
        Thread.sleep(2000);
    }

    /**
     * Simulates toggling the job processing condition while jobs are being processed.
     */
    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testJobProcessingConditionToggle() throws Exception {
        final AtomicInteger processed = new AtomicInteger(0);
        final int jobCount = 10;
        final String uniqueTopic = TOPIC + "/race/" + System.currentTimeMillis();

        this.registerJobConsumer(uniqueTopic, job -> {
            processed.incrementAndGet();
            // Wait a bit to allow toggling
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return JobConsumer.JobResult.OK;
        });

        // Add jobs
        for (int i = 0; i < jobCount; i++) {
            jobManager.addJob(uniqueTopic, Collections.singletonMap("id", i));
        }

        // Wait for some jobs to start processing
        while (processed.get() < 3) {
            Thread.sleep(100);
        }

        // Disable job processing
        setJobProcessingEnabled(false);

        // Wait to ensure jobs are paused
        int processedAfterDisable = processed.get();
        Thread.sleep(1000);
        assertEquals("No jobs should be processed while disabled", processedAfterDisable, processed.get());

        // Re-enable job processing
        setJobProcessingEnabled(true);

        // Wait for all jobs to finish
        while (processed.get() < jobCount) {
            Thread.sleep(100);
        }
        assertEquals("All jobs should eventually be processed", jobCount, processed.get());
    }

    /**
     * Simulates topology changes while jobs are being processed.
     */
    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testTopologyChangeDuringJobProcessing() throws Exception {
        final AtomicInteger processed = new AtomicInteger(0);
        final int jobCount = 5;

        this.registerJobConsumer(TOPIC, job -> {
            processed.incrementAndGet();
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return JobConsumer.JobResult.OK;
        });

        // Add jobs
        for (int i = 0; i < jobCount; i++) {
            jobManager.addJob(TOPIC, Collections.singletonMap("id", i));
        }

        // Simulate topology change by registering/unregistering a dummy service
        BundleContext ctx = bundleContext;
        ServiceRegistration<?> reg = ctx.registerService(Object.class, new Object(), null);
        Thread.sleep(500);
        reg.unregister();

        // Wait for all jobs to finish
        while (processed.get() < jobCount) {
            Thread.sleep(50);
        }
        assertEquals("All jobs should be processed despite topology changes", jobCount, processed.get());
    }

    /**
     * Simulates rapid toggling of both condition and topology during job processing.
     */
    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testRaceConditionWithConditionAndTopology() throws Exception {
        final AtomicInteger processed = new AtomicInteger(0);
        final int jobCount = 8;
        final String uniqueTopic = TOPIC + "/race/" + System.currentTimeMillis();

        this.registerJobConsumer(uniqueTopic, job -> {
            processed.incrementAndGet();
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return JobConsumer.JobResult.OK;
        });

        // Add jobs
        for (int i = 0; i < jobCount; i++) {
            jobManager.addJob(uniqueTopic, Collections.singletonMap("id", i));
        }

        // Rapidly toggle condition and topology
        BundleContext ctx = bundleContext;
        for (int i = 0; i < 3; i++) {
            setJobProcessingEnabled(false);
            ServiceRegistration<?> reg = ctx.registerService(Object.class, new Object(), null);
            Thread.sleep(300);
            setJobProcessingEnabled(true);
            reg.unregister();
            Thread.sleep(300);
        }

        // Wait for all jobs to finish
        long start = System.currentTimeMillis();
        long maxWait = 10000; // 5 seconds
        while (processed.get() < jobCount && (System.currentTimeMillis() - start) < maxWait) {
            Thread.sleep(100);
        }
        assertEquals("All jobs should be processed after race conditions", jobCount, processed.get());
    }

    // Helper to toggle the job processing condition
    private void setJobProcessingEnabled(boolean enabled) throws Exception {
        String conditionId = "org.apache.sling.event.jobs.processing.enabled";
        if (enabled) {
            if (jobProcessingConditionReg == null) {
                Dictionary<String, Object> props = new Hashtable<>();
                props.put("osgi.condition.id", conditionId);
                jobProcessingConditionReg = bundleContext.registerService(
                        Condition.class, UNCONDITIONAL_CONDITION, props
                );
            }
        } else {
            if (jobProcessingConditionReg != null) {
                jobProcessingConditionReg.unregister();
                jobProcessingConditionReg = null;
            }
        }
        Thread.sleep(300); // Wait for the system to react
    }

    private static final Condition UNCONDITIONAL_CONDITION = new Condition() {};
}
