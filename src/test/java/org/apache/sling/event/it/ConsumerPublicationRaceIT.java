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

import javax.inject.Inject;

import java.util.Arrays;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.sling.discovery.DiscoveryService;
import org.apache.sling.discovery.PropertyProvider;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationAdmin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.options;

/**
 * Reproduces lost consumer-capability publication when topic publication is
 * re-enabled concurrently with a consumer binding for a previously absent topic.
 *
 * <p>{@code JobConsumerManager} calculates the advertised topics before registering
 * its Discovery property provider, but assigns {@code propagationService} only
 * after {@code registerService()} returns. A consumer binding in that interval
 * mistakes the null handle for disabled publication: it clears the advertised
 * topics and skips the update notification. Completing registration does not
 * recalculate or republish the topics.
 *
 * <p>The consumer remains available locally, but Discovery can retain an older
 * advertisement or read no topics. Newly bound topics should become routable;
 * instead, subsequent {@code addJob()} calls for missing topics persist jobs
 * without a target. Submission does not refresh the capabilities, so the problem
 * can persist after both registration calls have finished, without blocked threads.
 *
 * <p>This regression test uses real SCR and Discovery services and requires both
 * topics to be advertised after the overlapping registrations finish. The arriving
 * consumer's job must execute without an additional configuration update to repair
 * publication.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class ConsumerPublicationRaceIT extends AbstractJobHandlingIT {

    private static final String CONSUMER_MANAGER = "org.apache.sling.event.impl.jobs.JobConsumerManager";
    private static final String TOPICS_PROPERTY = "org.apache.sling.event.jobs.consumer.topics";
    private static final String PHASE_PROPERTY = "publication.race.phase";
    private static final String EXISTING_TOPIC = "sling/test/publication/existing";
    private static final String ARRIVING_TOPIC = "sling/test/publication/arriving";
    private static final long TIMEOUT_SECONDS = 30;

    @Inject
    private ConfigurationAdmin configurationAdmin;

    @Inject
    private DiscoveryService discovery;

    @Configuration
    public Option[] configuration() {
        return options(baseConfiguration());
    }

    /**
     * Verifies that a concurrently bound consumer is advertised and its job executes
     * after publication is re-enabled, without an explicit repair update.
     *
     * <p>The test thread drives configuration changes and checks the results. The
     * Configuration Admin callback thread enables publication and is paused inside
     * registerService(). A separate worker then registers the arriving consumer,
     * making SCR bind it before the publisher's registration handle is assigned.
     * Job-processing threads signal the execution latches independently.
     *
     * <p>The enforced ordering is:
     * <pre>
     * CM callback: calculate topics -> register provider -> [paused] -> assign handle
     * Consumer worker:                                     bind consumer
     * Test thread:                  await pause -> await bind -> release pause
     * </pre>
     */
    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testReenablingPublicationIncludesConcurrentConsumer() throws Exception {
        final org.osgi.service.cm.Configuration config = configurationAdmin.getConfiguration(CONSUMER_MANAGER, null);
        final Dictionary<String, Object> originalProperties = config.getProperties();

        // These are signalled by job execution, not by service registration or topology delivery.
        final CountDownLatch existingProcessed = new CountDownLatch(1);
        final CountDownLatch arrivingProcessed = new CountDownLatch(1);
        final AtomicReference<String> processedJobId = new AtomicReference<>();

        // Configuration updates are asynchronous. SCR's subsequent service MODIFIED event
        // confirms that the corresponding modified() callback has returned.
        final CountDownLatch disabledUpdated = new CountDownLatch(1);
        final CountDownLatch enabledUpdated = new CountDownLatch(1);

        // The first latch announces the open race window; the second keeps it open
        // until the test has observed the consumer bind.
        final CountDownLatch providerRegistering = new CountDownLatch(1);
        final CountDownLatch finishProviderRegistration = new CountDownLatch(1);
        // Arm only after disabling completes, and intercept exactly one registration.
        final AtomicBoolean pauseRegistration = new AtomicBoolean();
        final AtomicReference<PropertyProvider> pendingProvider = new AtomicReference<>();
        final AtomicReference<ServiceReference<?>> providerReference = new AtomicReference<>();
        // Listener exceptions are handled by OSGi, so carry failures back to the test thread.
        final AtomicReference<Throwable> listenerFailure = new AtomicReference<>();
        final AtomicReference<ServiceRegistration<JobConsumer>> arrivingRegistration = new AtomicReference<>();
        final ExecutorService consumerRegistrationThread = Executors.newSingleThreadExecutor();

        final ServiceListener listener = event -> {
            final ServiceReference<?> reference = event.getServiceReference();
            if (event.getType() == ServiceEvent.MODIFIED
                    && CONSUMER_MANAGER.equals(reference.getProperty("component.name"))) {
                final Object phase = reference.getProperty(PHASE_PROPERTY);
                if ("disabled".equals(phase)) {
                    disabledUpdated.countDown();
                } else if ("enabled".equals(phase)) {
                    enabledUpdated.countDown();
                }
            }
            if (event.getType() == ServiceEvent.REGISTERED
                    && TOPICS_PROPERTY.equals(reference.getProperty(PropertyProvider.PROPERTY_PROPERTIES))
                    && pauseRegistration.compareAndSet(true, false)) {
                // This executes on the CM callback thread. The provider is already visible
                // to OSGi, but synchronous event delivery prevents registerService() from
                // returning, so JobConsumerManager.propagationService is still null.
                providerReference.set(reference);
                final Object service = bundleContext.getService(reference);
                if (service instanceof PropertyProvider) {
                    pendingProvider.set((PropertyProvider) service);
                } else {
                    listenerFailure.set(new AssertionError("Registered property provider was unavailable"));
                }
                providerRegistering.countDown();
                try {
                    if (!finishProviderRegistration.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        listenerFailure.set(new AssertionError("Timed out waiting to finish provider registration"));
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    listenerFailure.set(e);
                }
            }
        };
        bundleContext.addServiceListener(listener);
        ServiceRegistration<JobConsumer> existingRegistration = null;
        ServiceReference<?> managerReference = null;
        try {
            // Establish working end-to-end processing before opening the race window.
            existingRegistration = registerConsumer(EXISTING_TOPIC, job -> {
                existingProcessed.countDown();
                return JobConsumer.JobResult.OK;
            });
            awaitAdvertisedTopic(EXISTING_TOPIC);
            assertNotNull(jobManager.addJob(EXISTING_TOPIC, null));
            await(existingProcessed, "Initial consumer did not execute");

            // Acquire before modified() holds SCR's state lock. Looking up the service
            // inside the pause can contend with that lock instead of testing consumer binding.
            final ServiceReference<?>[] managers = bundleContext.getAllServiceReferences(CONSUMER_MANAGER, null);
            assertNotNull("Consumer manager service is missing", managers);
            assertEquals(1, managers.length);
            managerReference = managers[0];
            final Object manager = bundleContext.getService(managerReference);
            assertNotNull("Consumer manager service is unavailable", manager);

            // Disable only capability publication: the existing consumer remains bound.
            // Re-enabling must calculate its topic before registering the new provider.
            config.update(configurationProperties(true, "disabled"));
            await(disabledUpdated, "Publication disabling did not finish");
            pauseRegistration.set(true);
            config.update(configurationProperties(false, "enabled"));
            await(providerRegistering, "Publication re-enabling did not enter provider registration");
            assertNotNull(pendingProvider.get());
            assertTrue(hasTopic(pendingProvider.get().getProperty(TOPICS_PROPERTY), EXISTING_TOPIC));

            // Do not use registerJobConsumer(): it waits for the very notification lost by this race.
            final Future<?> bound = consumerRegistrationThread.submit(() -> {
                arrivingRegistration.set(registerConsumer(ARRIVING_TOPIC, job -> {
                    processedJobId.set(job.getId());
                    arrivingProcessed.countDown();
                    return JobConsumer.JobResult.OK;
                }));
            });
            bound.get(TIMEOUT_SECONDS / 2, TimeUnit.SECONDS);
            // Service registration alone is insufficient proof: check SCR's local consumer
            // mapping while the CM thread is still paused.
            assertConsumerBoundLocally(manager, ARRIVING_TOPIC);

            // Do not require publication while registration is paused: a fix may defer it
            // until the handle is assigned. Afterward, both topics must reach Discovery.
            finishProviderRegistration.countDown();
            await(enabledUpdated, "Publication re-enabling did not finish");
            assertNull("Property-provider listener failed", listenerFailure.get());
            awaitAdvertisedTopic(ARRIVING_TOPIC);
            awaitAdvertisedTopic(EXISTING_TOPIC);

            // Discovery delivery and Event's topology processing are asynchronous. Verify
            // eventual execution of this exact job, without forcing another publication.
            final Job job = jobManager.addJob(ARRIVING_TOPIC, null);
            assertNotNull(job);
            await(arrivingProcessed, "The concurrently bound consumer did not execute its job");
            assertEquals(job.getId(), processedJobId.get());
        } finally {
            // Always release the CM callback, even if an assertion fails inside the race
            // window. Stop the worker before reading its registration for cleanup.
            finishProviderRegistration.countDown();
            consumerRegistrationThread.shutdownNow();
            try {
                assertTrue(
                        "Consumer-registration thread did not terminate",
                        consumerRegistrationThread.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            } finally {
                bundleContext.removeServiceListener(listener);
                if (providerReference.get() != null) {
                    bundleContext.ungetService(providerReference.get());
                }
                if (managerReference != null) {
                    bundleContext.ungetService(managerReference);
                }
                if (arrivingRegistration.get() != null) {
                    arrivingRegistration.get().unregister();
                }
                if (existingRegistration != null) {
                    existingRegistration.unregister();
                }
                if (originalProperties == null) {
                    config.delete();
                } else {
                    config.update(originalProperties);
                }
            }
        }
    }

    private ServiceRegistration<JobConsumer> registerConsumer(final String topic, final JobConsumer consumer) {
        final Dictionary<String, Object> properties = new Hashtable<>();
        properties.put(JobConsumer.PROPERTY_TOPICS, topic);
        return bundleContext.registerService(JobConsumer.class, consumer, properties);
    }

    private Dictionary<String, Object> configurationProperties(final boolean disabled, final String phase) {
        final Dictionary<String, Object> properties = new Hashtable<>();
        properties.put("job.consumermanager.allowlist", new String[] {"*"});
        properties.put("job.consumermanager.denylist", disabled ? new String[] {"*"} : new String[0]);
        properties.put(PHASE_PROPERTY, phase);
        return properties;
    }

    private void assertConsumerBoundLocally(final Object manager, final String topic) throws Exception {
        // The implementation package is private; inspect the real service without exporting it for this test.
        assertNotNull(
                "SCR did not bind the arriving consumer before provider registration finished",
                manager.getClass().getMethod("getExecutor", String.class).invoke(manager, topic));
    }

    private void awaitAdvertisedTopic(final String topic) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        String topics;
        do {
            topics = discovery.getTopology().getLocalInstance().getProperty(TOPICS_PROPERTY);
            if (hasTopic(topics, topic)) {
                return;
            }
            Thread.sleep(25);
        } while (System.nanoTime() < deadline);
        assertTrue("Discovery did not advertise " + topic + "; topics=" + topics, hasTopic(topics, topic));
    }

    private static boolean hasTopic(final String topics, final String topic) {
        return topics != null && Arrays.asList(topics.split(",")).contains(topic);
    }

    private static void await(final CountDownLatch latch, final String message) throws InterruptedException {
        assertTrue(message, latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
}
