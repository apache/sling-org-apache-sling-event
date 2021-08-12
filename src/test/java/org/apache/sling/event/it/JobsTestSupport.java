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

import java.util.Objects;

import javax.inject.Inject;

import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.testing.paxexam.TestSupport;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.ModifiableCompositeOption;
import org.ops4j.pax.exam.options.OptionalCompositeOption;
import org.ops4j.pax.exam.options.extra.VMOption;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sling.testing.paxexam.SlingOptions.slingEvent;
import static org.apache.sling.testing.paxexam.SlingOptions.slingQuickstartOakTar;
import static org.apache.sling.testing.paxexam.SlingOptions.versionResolver;
import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.vmOption;
import static org.ops4j.pax.exam.CoreOptions.when;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;

public abstract class JobsTestSupport extends TestSupport {

    @Inject
    protected BundleContext bundleContext;

    @Inject
    protected JobManager jobManager;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    protected long backgroundLoadDelay() {
        return 3L;
    }

    public ModifiableCompositeOption baseConfiguration() {
        return composite(
            super.baseConfiguration(),
            slingQuickstart(),
            // Sling Event
            testBundle("bundle.filename"),
            slingEvent(),
            // testing configurations
            newConfiguration("org.apache.sling.event.impl.jobs.jcr.PersistenceHandler")
                .put(JobManagerConfiguration.PROPERTY_BACKGROUND_LOAD_DELAY, backgroundLoadDelay())
                .put("startup.delay", 1L)
                .asOption(),
            newConfiguration("org.apache.sling.commons.scheduler.impl.QuartzScheduler")
                .put("allowedPoolNames", new String[]{"oak"})
                .asOption(),
            // this test code uses loginAdministrative!
            newConfiguration("org.apache.sling.jcr.base.internal.LoginAdminWhitelist")
                .put("whitelist.bundles.regexp", "PAXEXAM-PROBE-.*")
                .asOption(),
            // otherwise we get ignored events
            newConfiguration("org.apache.felix.eventadmin.impl.EventAdmin")
                .put("org.apache.felix.eventadmin.IgnoreTimeout", "*")
                .asOption(),
            // testing
            mavenBundle().groupId("org.apache.sling").artifactId("org.apache.sling.testing.tools").versionAsInProject(),
            mavenBundle().groupId("org.apache.sling").artifactId("org.apache.sling.commons.json").versionAsInProject(),
            junitBundles(),
            jacoco() // remove with Testing PaxExam 4.0
        ).remove(
            mavenBundle().groupId("org.apache.sling").artifactId("org.apache.sling.event").version(versionResolver)
        );
    }

    protected Option slingQuickstart() {
        final String workingDirectory = workingDirectory();
        final int httpPort = findFreePort();
        return composite(
            slingQuickstartOakTar(workingDirectory, httpPort)
        );
    }

    // remove with Testing PaxExam 4.0
    protected OptionalCompositeOption jacoco() {
        final String jacocoCommand = System.getProperty("jacoco.command");
        final VMOption option = Objects.nonNull(jacocoCommand) && !jacocoCommand.trim().isEmpty() ? vmOption(jacocoCommand) : null;
        return when(Objects.nonNull(option)).useOptions(option);
    }

}
