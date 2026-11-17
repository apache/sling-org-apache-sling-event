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
package org.apache.sling.event.impl.jobs.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.sling.discovery.ClusterView;
import org.apache.sling.discovery.InstanceDescription;
import org.apache.sling.discovery.TopologyView;
import org.apache.sling.event.jobs.QueueConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class TopologyCapabilitiesTest {

    private TopologyCapabilities caps;

    @Before
    public void setup() {
        // local cluster view
        final ClusterView cv = Mockito.mock(ClusterView.class);
        Mockito.when(cv.getId()).thenReturn("cluster");

        // local description
        final InstanceDescription local = Mockito.mock(InstanceDescription.class);
        Mockito.when(local.isLeader()).thenReturn(true);
        Mockito.when(local.getSlingId()).thenReturn("local");
        Mockito.when(local.getProperty(TopologyCapabilities.PROPERTY_TOPICS))
                .thenReturn("foo,bar/*,a/**,d/1/2,d/1/*,d/**");
        Mockito.when(local.getClusterView()).thenReturn(cv);

        // topology view
        final TopologyView tv = Mockito.mock(TopologyView.class);
        Mockito.when(tv.getInstances()).thenReturn(Collections.singleton(local));
        Mockito.when(tv.getLocalInstance()).thenReturn(local);

        final JobManagerConfiguration config = Mockito.mock(JobManagerConfiguration.class);

        caps = new TopologyCapabilities(tv, config);
    }

    @Test
    public void testMatching() {
        assertEquals(1, caps.getPotentialTargets("foo").size());
        assertEquals(0, caps.getPotentialTargets("foo/a").size());
        assertEquals(0, caps.getPotentialTargets("bar").size());
        assertEquals(1, caps.getPotentialTargets("bar/foo").size());
        assertEquals(0, caps.getPotentialTargets("bar/foo/a").size());
        assertEquals(1, caps.getPotentialTargets("a/b").size());
        assertEquals(1, caps.getPotentialTargets("a/b(c").size());
        assertEquals(0, caps.getPotentialTargets("x").size());
        assertEquals(0, caps.getPotentialTargets("x/y").size());
        assertEquals(1, caps.getPotentialTargets("d/1/2").size());
    }

    @Test
    public void testConcurrentDetectTargetIsRaceFree() throws Exception {
        final int instanceCount = 50;
        final int threads = 10;
        final int callsPerThread = 500;
        final int expectedPerTarget = (threads * callsPerThread) / instanceCount;

        final ClusterView cv = Mockito.mock(ClusterView.class);
        Mockito.when(cv.getId()).thenReturn("cluster");

        final Set<InstanceDescription> instances = new LinkedHashSet<>();
        for (int i = 0; i < instanceCount; i++) {
            final InstanceDescription desc = Mockito.mock(InstanceDescription.class);
            Mockito.when(desc.getSlingId()).thenReturn("instance-" + i);
            Mockito.when(desc.getProperty(TopologyCapabilities.PROPERTY_TOPICS)).thenReturn("foo");
            Mockito.when(desc.getClusterView()).thenReturn(cv);
            instances.add(desc);
        }
        final InstanceDescription local = instances.iterator().next();
        Mockito.when(local.isLeader()).thenReturn(true);

        final TopologyView tv = Mockito.mock(TopologyView.class);
        Mockito.when(tv.getInstances()).thenReturn(instances);
        Mockito.when(tv.getLocalInstance()).thenReturn(local);

        final JobManagerConfiguration config = Mockito.mock(JobManagerConfiguration.class);
        final TopologyCapabilities localCaps = new TopologyCapabilities(tv, config);

        final InternalQueueConfiguration queueConfig = Mockito.mock(InternalQueueConfiguration.class);
        Mockito.when(queueConfig.getType()).thenReturn(QueueConfiguration.Type.UNORDERED);
        Mockito.when(queueConfig.isPreferRunOnCreationInstance()).thenReturn(false);
        final QueueConfigurationManager.QueueInfo queueInfo = new QueueConfigurationManager.QueueInfo();
        queueInfo.queueConfiguration = queueConfig;
        queueInfo.queueName = "foo";

        // count how often each instance is selected as a target
        final Map<String, AtomicInteger> hits = new ConcurrentHashMap<>();
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            // release all threads at once (via the latch) to maximize contention on detectTarget
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < callsPerThread; i++) {
                        final String target = localCaps.detectTarget("foo", null, queueInfo);
                        hits.computeIfAbsent(target, k -> new AtomicInteger()).incrementAndGet();
                    }
                    return null;
                }));
            }
            start.countDown();
            // wait for all callers to finish (and surface any exception thrown in a worker)
            for (final Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        // total calls are a whole multiple of the target count, so a correct atomic round robin
        // hits every target exactly the same number of times; a lost update would skew the counts
        assertEquals(instanceCount, hits.size());
        for (final AtomicInteger hit : hits.values()) {
            assertEquals(expectedPerTarget, hit.get());
        }
    }
}
