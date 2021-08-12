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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.sling.discovery.PropertyProvider;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyEventListener;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.apache.sling.event.jobs.consumer.JobExecutor;
import org.junit.Before;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;

public abstract class AbstractJobHandlingTest extends JobsTestSupport {

    protected static final int DEFAULT_TEST_TIMEOUT = 1000*60*5;

    protected List<ServiceRegistration<?>> registrations = new ArrayList<>();

    protected void sleep(final long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // ignore
        }
    }

    @Before
    public void setup() throws IOException {
        log.info("starting setup");
        registerTopologyListener();
    }

    protected AtomicReference<TopologyEvent> lastTopologyEvent = new AtomicReference<>();

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
                if (isTopologyInitialized()) {
                    return;
                }
                log.info("waitConsumerChangeCount (topology not yet initialized)");
            } else {
                log.info("waitConsumerChangeCount (is={}, expected={})",cc, minimum);
            }
            sleep(50);
        } while ( true );
    }

    protected boolean isTopologyInitialized() {
        final TopologyEvent event = lastTopologyEvent.get();
        return (event != null) && (event.getNewView() != null);
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

    protected void registerTopologyListener() {
        final Dictionary<String, Object> props = new Hashtable<>();
        final ServiceRegistration<TopologyEventListener> reg = this.bc.registerService(TopologyEventListener.class,
                new TopologyEventListener() {

                    @Override
                    public void handleTopologyEvent(TopologyEvent event) {
                        log.info("handleTopologyEvent : GOT EVENT : " + event);
                        lastTopologyEvent.set(event);
                    }
                }, props);
        this.registrations.add(reg);
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
