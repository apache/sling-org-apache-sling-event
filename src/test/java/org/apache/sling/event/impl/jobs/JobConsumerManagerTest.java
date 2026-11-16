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
package org.apache.sling.event.impl.jobs;

import java.lang.annotation.Annotation;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.sling.discovery.PropertyProvider;
import org.apache.sling.event.impl.TestUtil;
import org.apache.sling.event.impl.jobs.config.TopologyCapabilities;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.apache.sling.event.jobs.consumer.JobExecutor;
import org.junit.Test;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JobConsumerManagerTest {

    private JobConsumerManager.Config getDefaultConfig() {
        return new JobConsumerManager.Config() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return JobConsumerManager.Config.class;
            }

            @Override
            public boolean org_apache_sling_installer_configuration_persist() {
                return false;
            }

            @Override
            public String[] job_consumermanager_allowlist() {
                return new String[] {"*"};
            }

            @Override
            public String[] job_consumermanager_denylist() {
                return null;
            }
        };
    }

    private JobConsumerManager.DeprecatedConfig getDeprecatedConfig() {
        return new JobConsumerManager.DeprecatedConfig() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return JobConsumerManager.DeprecatedConfig.class;
            }

            @Override
            public String[] job_consumermanager_whitelist() {
                return null;
            }

            @Override
            public String[] job_consumermanager_blacklist() {
                return null;
            }
        };
    }

    @Test
    public void testSimpleMappingConsumer() {
        final BundleContext bc = Mockito.mock(BundleContext.class);
        final JobConsumerManager jcs = new JobConsumerManager();
        jcs.activate(bc, getDefaultConfig(), getDeprecatedConfig());

        final JobConsumer jc1 = Mockito.mock(JobConsumer.class);
        final ServiceReference ref1 = Mockito.mock(ServiceReference.class);
        Mockito.when(ref1.getProperty(JobConsumer.PROPERTY_TOPICS)).thenReturn("a/b");
        Mockito.when(ref1.getProperty(Constants.SERVICE_RANKING)).thenReturn(1);
        Mockito.when(ref1.getProperty(Constants.SERVICE_ID)).thenReturn(1L);
        Mockito.when(bc.getService(ref1)).thenReturn(jc1);
        jcs.bindJobConsumer(ref1);

        assertNotNull(jcs.getExecutor("a/b"));
        assertNull(jcs.getExecutor("a"));
        assertNull(jcs.getExecutor("a/c"));
        assertNull(jcs.getExecutor("a/b/a"));
    }

    @Test
    public void testCategoryMappingConsumer() {
        final BundleContext bc = Mockito.mock(BundleContext.class);
        final JobConsumerManager jcs = new JobConsumerManager();
        jcs.activate(bc, getDefaultConfig(), getDeprecatedConfig());

        final JobConsumer jc1 = Mockito.mock(JobConsumer.class);
        final ServiceReference ref1 = Mockito.mock(ServiceReference.class);
        Mockito.when(ref1.getProperty(JobConsumer.PROPERTY_TOPICS)).thenReturn("a/*");
        Mockito.when(ref1.getProperty(Constants.SERVICE_RANKING)).thenReturn(1);
        Mockito.when(ref1.getProperty(Constants.SERVICE_ID)).thenReturn(1L);
        Mockito.when(bc.getService(ref1)).thenReturn(jc1);
        jcs.bindJobConsumer(ref1);

        assertNotNull(jcs.getExecutor("a/b"));
        assertNull(jcs.getExecutor("a"));
        assertNotNull(jcs.getExecutor("a/c"));
        assertNull(jcs.getExecutor("a/b/a"));
    }

    @Test
    public void testSubCategoryMappingConsumer() {
        final BundleContext bc = Mockito.mock(BundleContext.class);
        final JobConsumerManager jcs = new JobConsumerManager();
        jcs.activate(bc, getDefaultConfig(), getDeprecatedConfig());

        final JobConsumer jc1 = Mockito.mock(JobConsumer.class);
        final ServiceReference ref1 = Mockito.mock(ServiceReference.class);
        Mockito.when(ref1.getProperty(JobConsumer.PROPERTY_TOPICS)).thenReturn("a/**");
        Mockito.when(ref1.getProperty(Constants.SERVICE_RANKING)).thenReturn(1);
        Mockito.when(ref1.getProperty(Constants.SERVICE_ID)).thenReturn(1L);
        Mockito.when(bc.getService(ref1)).thenReturn(jc1);
        jcs.bindJobConsumer(ref1);

        assertNotNull(jcs.getExecutor("a/b"));
        assertNull(jcs.getExecutor("a"));
        assertNotNull(jcs.getExecutor("a/c"));
        assertNotNull(jcs.getExecutor("a/b/a"));
    }

    @Test
    public void testSimpleMappingExecutor() {
        final BundleContext bc = Mockito.mock(BundleContext.class);
        final JobConsumerManager jcs = new JobConsumerManager();
        jcs.activate(bc, getDefaultConfig(), getDeprecatedConfig());

        final JobExecutor jc1 = Mockito.mock(JobExecutor.class);
        final ServiceReference ref1 = Mockito.mock(ServiceReference.class);
        Mockito.when(ref1.getProperty(JobConsumer.PROPERTY_TOPICS)).thenReturn("a/b");
        Mockito.when(ref1.getProperty(Constants.SERVICE_RANKING)).thenReturn(1);
        Mockito.when(ref1.getProperty(Constants.SERVICE_ID)).thenReturn(1L);
        Mockito.when(bc.getService(ref1)).thenReturn(jc1);
        jcs.bindJobExecutor(ref1);

        assertNotNull(jcs.getExecutor("a/b"));
        assertNull(jcs.getExecutor("a"));
        assertNull(jcs.getExecutor("a/c"));
        assertNull(jcs.getExecutor("a/b/a"));
    }

    @Test
    public void testCategoryMappingExecutor() {
        final BundleContext bc = Mockito.mock(BundleContext.class);
        final JobConsumerManager jcs = new JobConsumerManager();
        jcs.activate(bc, getDefaultConfig(), getDeprecatedConfig());

        final JobExecutor jc1 = Mockito.mock(JobExecutor.class);
        final ServiceReference ref1 = Mockito.mock(ServiceReference.class);
        Mockito.when(ref1.getProperty(JobExecutor.PROPERTY_TOPICS)).thenReturn("a/*");
        Mockito.when(ref1.getProperty(Constants.SERVICE_RANKING)).thenReturn(1);
        Mockito.when(ref1.getProperty(Constants.SERVICE_ID)).thenReturn(1L);
        Mockito.when(bc.getService(ref1)).thenReturn(jc1);
        jcs.bindJobExecutor(ref1);

        assertNotNull(jcs.getExecutor("a/b"));
        assertNull(jcs.getExecutor("a"));
        assertNotNull(jcs.getExecutor("a/c"));
        assertNull(jcs.getExecutor("a/b/a"));
    }

    @Test
    public void testSubCategoryMappingExecutor() {
        final BundleContext bc = Mockito.mock(BundleContext.class);
        final JobConsumerManager jcs = new JobConsumerManager();
        jcs.activate(bc, getDefaultConfig(), getDeprecatedConfig());

        final JobExecutor jc1 = Mockito.mock(JobExecutor.class);
        final ServiceReference ref1 = Mockito.mock(ServiceReference.class);
        Mockito.when(ref1.getProperty(JobExecutor.PROPERTY_TOPICS)).thenReturn("a/**");
        Mockito.when(ref1.getProperty(Constants.SERVICE_RANKING)).thenReturn(1);
        Mockito.when(ref1.getProperty(Constants.SERVICE_ID)).thenReturn(1L);
        Mockito.when(bc.getService(ref1)).thenReturn(jc1);
        jcs.bindJobExecutor(ref1);

        assertNotNull(jcs.getExecutor("a/b"));
        assertNull(jcs.getExecutor("a"));
        assertNotNull(jcs.getExecutor("a/c"));
        assertNotNull(jcs.getExecutor("a/b/a"));
    }

    @Test
    public void testRanking() {
        final BundleContext bc = Mockito.mock(BundleContext.class);
        final JobConsumerManager jcs = new JobConsumerManager();
        jcs.activate(bc, getDefaultConfig(), getDeprecatedConfig());

        final JobExecutor jc1 = Mockito.mock(JobExecutor.class);
        final JobExecutor jc2 = Mockito.mock(JobExecutor.class);
        final JobExecutor jc3 = Mockito.mock(JobExecutor.class);
        final JobExecutor jc4 = Mockito.mock(JobExecutor.class);
        final ServiceReference ref1 = Mockito.mock(ServiceReference.class);
        Mockito.when(ref1.getProperty(JobExecutor.PROPERTY_TOPICS)).thenReturn("a/b");
        Mockito.when(ref1.getProperty(Constants.SERVICE_RANKING)).thenReturn(1);
        Mockito.when(ref1.getProperty(Constants.SERVICE_ID)).thenReturn(1L);
        Mockito.when(bc.getService(ref1)).thenReturn(jc1);
        jcs.bindJobExecutor(ref1);
        assertEquals(jc1, jcs.getExecutor("a/b"));

        final ServiceReference ref2 = Mockito.mock(ServiceReference.class);
        Mockito.when(ref2.getProperty(JobExecutor.PROPERTY_TOPICS)).thenReturn("a/b");
        Mockito.when(ref2.getProperty(Constants.SERVICE_RANKING)).thenReturn(10);
        Mockito.when(ref2.getProperty(Constants.SERVICE_ID)).thenReturn(2L);
        Mockito.when(bc.getService(ref2)).thenReturn(jc2);
        jcs.bindJobExecutor(ref2);
        assertEquals(jc2, jcs.getExecutor("a/b"));

        final ServiceReference ref3 = Mockito.mock(ServiceReference.class);
        Mockito.when(ref3.getProperty(JobExecutor.PROPERTY_TOPICS)).thenReturn("a/b");
        Mockito.when(ref3.getProperty(Constants.SERVICE_RANKING)).thenReturn(5);
        Mockito.when(ref3.getProperty(Constants.SERVICE_ID)).thenReturn(3L);
        Mockito.when(bc.getService(ref3)).thenReturn(jc3);
        jcs.bindJobExecutor(ref3);
        assertEquals(jc2, jcs.getExecutor("a/b"));

        final ServiceReference ref4 = Mockito.mock(ServiceReference.class);
        Mockito.when(ref4.getProperty(JobExecutor.PROPERTY_TOPICS)).thenReturn("a/b");
        Mockito.when(ref4.getProperty(Constants.SERVICE_RANKING)).thenReturn(5);
        Mockito.when(ref4.getProperty(Constants.SERVICE_ID)).thenReturn(4L);
        Mockito.when(bc.getService(ref4)).thenReturn(jc4);
        jcs.bindJobExecutor(ref4);
        assertEquals(jc2, jcs.getExecutor("a/b"));

        jcs.unbindJobExecutor(ref2);
        assertEquals(jc3, jcs.getExecutor("a/b"));

        jcs.unbindJobExecutor(ref3);
        assertEquals(jc4, jcs.getExecutor("a/b"));
    }

    @Test
    public void testRegistrationDiagnosticsObserveLostPublicationWithoutRepairingIt() throws Exception {
        final BundleContext context = Mockito.mock(BundleContext.class);
        final JobConsumerManager manager = new JobConsumerManager();
        final Logger logger = Mockito.mock(Logger.class);
        Mockito.when(logger.isDebugEnabled()).thenReturn(true);
        TestUtil.setFieldValue(manager, "logger", logger);
        final ServiceRegistration<?> registration = Mockito.mock(ServiceRegistration.class);
        manager.bindJobExecutor(executorReference("a/existing", 1L));
        final ServiceReference<JobExecutor> arriving = executorReference("b/arriving", 2L);
        final ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Mockito.when(context.registerService(
                            Mockito.eq(PropertyProvider.class), Mockito.any(PropertyProvider.class), Mockito.any()))
                    .thenAnswer(invocation -> {
                        final PropertyProvider provider = invocation.getArgument(1);
                        assertEquals("a/existing", provider.getProperty(TopologyCapabilities.PROPERTY_TOPICS));
                        // Keep registration open until a different thread has completed the binding.
                        worker.submit(() -> manager.bindJobExecutor(arriving)).get(5, TimeUnit.SECONDS);
                        assertTrue(
                                manager.getPublicationDiagnostics("b/arriving").contains("registrationPending=true"));
                        assertNull(provider.getProperty(TopologyCapabilities.PROPERTY_TOPICS));
                        return registration;
                    });

            manager.activate(context, getDefaultConfig(), getDeprecatedConfig());

            assertNull(manager.getTopics());
            final String state = manager.getPublicationDiagnostics("b/arriving");
            assertTrue(state.contains("localConsumerBound=true"));
            assertTrue(state.contains("publicationAllowed=true"));
            assertTrue(state.contains("registrationPending=false"));
            assertTrue(state.contains("registrationWindowChanges=1"));
            Mockito.verify(logger)
                    .info(
                            "Consumer topic provider registration finished; cycle={}, completed={}, "
                                    + "windowChanges={}, expectedTopics={}, actualTopics={}, topicsConsistent={}",
                            1L,
                            true,
                            1,
                            "a/existing,b/arriving",
                            null,
                            false);
            Mockito.verify(registration, Mockito.never()).setProperties(Mockito.any());
            Mockito.verify(context, Mockito.never()).getService(Mockito.any());
        } finally {
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testPublicationDiagnosticsApplyFiltersToConsumerPatterns() {
        final JobConsumerManager manager = new JobConsumerManager();
        final BundleContext context = Mockito.mock(BundleContext.class);
        Mockito.when(context.registerService(
                        Mockito.eq(PropertyProvider.class), Mockito.any(PropertyProvider.class), Mockito.any()))
                .thenAnswer(invocation -> Mockito.mock(ServiceRegistration.class));
        final JobConsumerManager.Config config = Mockito.spy(getDefaultConfig());
        Mockito.when(config.job_consumermanager_denylist()).thenReturn(new String[] {"blocked/*"});
        manager.activate(context, config, getDeprecatedConfig());
        manager.bindJobExecutor(executorReference("allowed/*", 1L));
        manager.bindJobExecutor(executorReference("blocked/**", 2L));

        assertTrue(manager.getPublicationDiagnostics("allowed/job").contains("localConsumerBound=true"));
        assertTrue(manager.getPublicationDiagnostics("allowed/job").contains("publicationAllowed=true"));
        assertTrue(manager.getPublicationDiagnostics("blocked/deep/job").contains("localConsumerBound=true"));
        assertTrue(manager.getPublicationDiagnostics("blocked/deep/job").contains("publicationAllowed=false"));
        assertTrue(manager.getPublicationDiagnostics("allowed/deep/job").contains("localConsumerBound=false"));
        Mockito.verify(context, Mockito.never()).getService(Mockito.any());
    }

    @Test
    public void testRegistrationFailureIsReportedAndPropagated() {
        final JobConsumerManager manager = new JobConsumerManager();
        final Logger logger = Mockito.mock(Logger.class);
        TestUtil.setFieldValue(manager, "logger", logger);
        final BundleContext context = Mockito.mock(BundleContext.class);
        final IllegalStateException failure = new IllegalStateException("Registration failed");
        Mockito.when(context.registerService(
                        Mockito.eq(PropertyProvider.class), Mockito.any(PropertyProvider.class), Mockito.any()))
                .thenThrow(failure);

        try {
            manager.activate(context, getDefaultConfig(), getDeprecatedConfig());
            fail("Registration failure must propagate");
        } catch (final IllegalStateException actual) {
            assertTrue(actual == failure);
        }

        assertTrue(manager.getPublicationDiagnostics("missing/job").contains("registrationPending=false"));
        Mockito.verify(logger)
                .info(
                        "Consumer topic provider registration finished; cycle={}, completed={}, "
                                + "windowChanges={}, expectedTopics={}, actualTopics={}, topicsConsistent={}",
                        1L,
                        false,
                        0,
                        "",
                        "",
                        true);
    }

    @SuppressWarnings("unchecked")
    private ServiceReference<JobExecutor> executorReference(final String topic, final long id) {
        final ServiceReference<JobExecutor> reference = Mockito.mock(ServiceReference.class);
        Mockito.when(reference.getProperty(JobExecutor.PROPERTY_TOPICS)).thenReturn(topic);
        Mockito.when(reference.getProperty(Constants.SERVICE_ID)).thenReturn(id);
        return reference;
    }
}
