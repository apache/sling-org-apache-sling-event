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


import static org.apache.sling.testing.paxexam.SlingOptions.backing;
import static org.apache.sling.testing.paxexam.SlingOptions.paxTinybundles;
import static org.apache.sling.testing.paxexam.SlingOptions.spyfly;
import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.keepCaches;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.CoreOptions.repository;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.factoryConfiguration;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;

import javax.inject.Inject;

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.discovery.PropertyProvider;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.apache.sling.event.jobs.consumer.JobExecutor;
import org.apache.sling.jcr.api.SlingRepository;
import org.apache.sling.testing.paxexam.SlingOptions;
import org.apache.sling.testing.paxexam.SlingVersionResolver;
import org.apache.sling.testing.paxexam.TestSupport;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.ModifiableCompositeOption;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.event.EventAdmin;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJobHandlingTest extends TestSupport {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final String PROPERTY_BUNDLE_FILENAME = "bundle.filename";

    protected static final int DEFAULT_TEST_TIMEOUT = 1000*60*5;

    @Inject
    protected EventAdmin eventAdmin;

    @Inject
    protected ConfigurationAdmin configAdmin;

    @Inject
    protected BundleContext bc;

    @Inject // just to block the dependencies
    protected SlingRepository repo;

    protected List<ServiceRegistration<?>> registrations = new ArrayList<>();

    public static SlingVersionResolver versionResolver = new SlingVersionResolver();

    @Configuration
    public Option[] config() {

        final String workingDirectory = workingDirectory(); // from TestSupport
        final int httpPort = findFreePort(); // from TestSupport

        final String bundleFileName = System.getProperty(PROPERTY_BUNDLE_FILENAME );
        final File bundleFile = new File( bundleFileName );
        if ( !bundleFile.canRead() ) {
            throw new IllegalArgumentException( "Cannot read from bundle file " + bundleFileName + " specified in the "
                + PROPERTY_BUNDLE_FILENAME + " system property" );
        }

        return options(
                newConfiguration("org.apache.sling.event.impl.jobs.jcr.PersistenceHandler")
                    .put(JobManagerConfiguration.PROPERTY_BACKGROUND_LOAD_DELAY, 3L)
                    .put("startup.delay", 1L)
                    .asOption(),
                baseConfiguration(),
                SlingOptions.slingQuickstartOakTar(workingDirectory, httpPort),
                SlingOptions.logback(), testBundle(PROPERTY_BUNDLE_FILENAME), // this bundle
                SlingOptions.slingDiscovery(),
                mavenBundle().groupId("org.apache.sling").artifactId("org.apache.sling.event.dea").version(versionResolver),
                mavenBundle().groupId("org.apache.felix").artifactId("org.apache.felix.inventory").version(versionResolver),
                mavenBundle().groupId("org.apache.sling").artifactId("org.apache.sling.serviceusermapper").version("1.5.2"),
                factoryConfiguration("org.apache.sling.jcr.repoinit.RepositoryInitializer")
                    .put("scripts", new String[]{"create service user sling-event\n\n  create path (sling:Folder) /var/eventing\n\n  set ACL for sling-event\n\n    allow   jcr:all     on /var/eventing\n\n  end"})
                    .asOption(),
                factoryConfiguration("org.apache.sling.serviceusermapping.impl.ServiceUserMapperImpl.amended")
                    .put("user.mapping", new String[]{"org.apache.sling.event=[sling-event]", "org.apache.sling.event.dea=[sling-event]"})
                    .asOption(),
                newConfiguration("org.apache.sling.commons.scheduler.impl.QuartzScheduler")
                    .put("allowedPoolNames",new String[] {"oak"})
                    .asOption(),
                // this test code uses loginAdministrative!
                newConfiguration("org.apache.sling.jcr.base.internal.LoginAdminWhitelist")
                    .put("whitelist.bundles.regexp", "PAXEXAM-PROBE-.*")
                    .asOption(),
                // otherwise we get ignored events
                newConfiguration("org.apache.felix.eventadmin.impl.EventAdmin")
                        .put("org.apache.felix.eventadmin.IgnoreTimeout", "*")
                        .asOption(),
                mavenBundle().groupId("org.apache.sling").artifactId("org.apache.sling.testing.tools").version("1.0.14"),
                mavenBundle().groupId("org.apache.sling").artifactId("org.apache.sling.commons.json").version("2.0.20"),
                junitBundles()
           );
    }

    protected ModifiableCompositeOption baseConfiguration() {
        return composite(
            failOnUnresolvedBundles(),
            keepCaches(),
            localMavenRepo(),
            repository("https://repo1.maven.org/maven2/").id("apache-snapshots").allowSnapshots(),
            CoreOptions.workingDirectory(workingDirectory()),
            mavenBundle().groupId("org.apache.sling").artifactId("org.apache.sling.testing.paxexam").versionAsInProject(),
            paxTinybundles(),
            backing(),
            spyfly()
        );
    }

    protected JobManager getJobManager() {
        JobManager result = null;
        int count = 0;
        do {
            final ServiceReference<JobManager> sr = this.bc.getServiceReference(JobManager.class);
            if ( sr != null ) {
                result = this.bc.getService(sr);
            } else {
                count++;
                if ( count == 10 ) {
                    break;
                }
                sleep(500);
            }

        } while ( result == null );
        return result;
    }

    protected void sleep(final long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // ignore
        }
    }

    public void setup() throws IOException {
        log.info("starting setup");
    }

    private int deleteCount;

    private void delete(final Resource rsrc )
    throws PersistenceException {
        final ResourceResolver resolver = rsrc.getResourceResolver();
        for(final Resource child : rsrc.getChildren()) {
            delete(child);
        }
        resolver.delete(rsrc);
        deleteCount++;
        if ( deleteCount >= 20 ) {
            resolver.commit();
            deleteCount = 0;
        }
    }

    public void cleanup() {
        log.info("starting cleanup");
        // clean job area
        final ServiceReference<ResourceResolverFactory> ref = this.bc.getServiceReference(ResourceResolverFactory.class);
        final ResourceResolverFactory factory = this.bc.getService(ref);
        ResourceResolver resolver = null;
        try {
            resolver = factory.getAdministrativeResourceResolver(null);
            final Resource rsrc = resolver.getResource("/var/eventing");
            if ( rsrc != null ) {
                delete(rsrc);
                resolver.commit();
            }
        } catch ( final LoginException le ) {
            // ignore
        } catch (final PersistenceException e) {
            // ignore
        } catch ( final Exception e ) {
            // sometimes an NPE is thrown from the repository, as we
            // are in the cleanup, we can ignore this
        } finally {
            if ( resolver != null ) {
                resolver.close();
            }
        }
        // unregister all services
        for(final ServiceRegistration<?> reg : this.registrations) {
            reg.unregister();
        }
        this.registrations.clear();

        // remove all configurations
        try {
            final org.osgi.service.cm.Configuration[] cfgs = this.configAdmin.listConfigurations(null);
            if ( cfgs != null ) {
                for(final org.osgi.service.cm.Configuration c : cfgs) {
                    try {
                        c.delete();
                    } catch (final IOException io) {
                        // ignore
                    }
                }
            }
        } catch (final IOException io) {
            // ignore
        } catch (final InvalidSyntaxException e) {
            // ignore
        }
        this.sleep(1000);
        log.info("cleanup completed");
    }

    /**
     * Helper method to register an event handler
     */
    protected ServiceRegistration<EventHandler> registerEventHandler(final String topic,
            final EventHandler handler) {
        final Dictionary<String, Object> props = new Hashtable<>();
        props.put(EventConstants.EVENT_TOPIC, topic);
        final ServiceRegistration<EventHandler> reg = this.bc.registerService(EventHandler.class,
                handler, props);
        this.registrations.add(reg);
        return reg;
    }

    protected long getConsumerChangeCount() {
        long result = -1;
        try {
            final Collection<ServiceReference<PropertyProvider>> refs = this.bc.getServiceReferences(PropertyProvider.class, "(changeCount=*)");
            log.info("GetConsumerChangeCount refs.size = {}", refs.size());
            if ( !refs.isEmpty() ) {
                result = refs.stream().mapToLong(r -> (Long) r.getProperty("changeCount")).max().getAsLong();
                log.info("GetConsumerChangeCount changeCount = {} ", result);
            }
        } catch ( final InvalidSyntaxException ignore ) {
            // ignore
        }
        return result;
    }

    protected void waitConsumerChangeCount(final long minimum) {
        do {
            final long cc = getConsumerChangeCount();
            if ( cc >= minimum ) {
                // we need to wait for the topology events (TODO)
                sleep(200);
                return;
            }
            sleep(50);
            log.info("waitConsumerChangeCount (is={}, expected={})",cc, minimum);
        } while ( true );
    }

    /**
     * Helper method to register a job consumer
     */
    protected ServiceRegistration<JobConsumer> registerJobConsumer(final String topic,
            final JobConsumer handler) {
        long cc = this.getConsumerChangeCount();
        final Dictionary<String, Object> props = new Hashtable<>();
        props.put(JobConsumer.PROPERTY_TOPICS, topic);
        final ServiceRegistration<JobConsumer> reg = this.bc.registerService(JobConsumer.class,
                handler, props);
        this.registrations.add(reg);
        log.info("registered JobConsumer for topic {} and changecount={}",topic, cc);
        this.waitConsumerChangeCount(cc + 1);
        log.info("registered2 JobConsumer for topic {} and changecount={}",topic, cc);
        return reg;
    }

    /**
     * Helper method to register a job executor
     */
    protected ServiceRegistration<JobExecutor> registerJobExecutor(final String topic,
            final JobExecutor handler) {
        long cc = this.getConsumerChangeCount();
        final Dictionary<String, Object> props = new Hashtable<>();
        props.put(JobConsumer.PROPERTY_TOPICS, topic);
        final ServiceRegistration<JobExecutor> reg = this.bc.registerService(JobExecutor.class,
                handler, props);
        this.registrations.add(reg);
        this.waitConsumerChangeCount(cc + 1);
        return reg;
    }

    protected void unregister(final ServiceRegistration<?> reg) {
        if ( reg != null ) {
            this.registrations.remove(reg);
            reg.unregister();
        }
    }
}
