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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.sling.discovery.ClusterView;
import org.apache.sling.discovery.InstanceDescription;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyEventListener;
import org.apache.sling.discovery.TopologyView;
import org.apache.sling.event.impl.TestUtil;
import org.apache.sling.event.impl.discovery.InitDelayingTopologyEventListener;
import org.junit.Test;
import org.mockito.Mockito;
import org.osgi.service.condition.Condition;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class JobManagerConfigurationTest {

    private TopologyView createView() {
        final TopologyView view = Mockito.mock(TopologyView.class);
        Mockito.when(view.isCurrent()).thenReturn(true);
        final InstanceDescription local = Mockito.mock(InstanceDescription.class);
        Mockito.when(local.isLeader()).thenReturn(true);
        Mockito.when(local.isLocal()).thenReturn(true);
        Mockito.when(local.getSlingId()).thenReturn("id");

        Mockito.when(view.getLocalInstance()).thenReturn(local);
        final ClusterView localView = Mockito.mock(ClusterView.class);
        Mockito.when(localView.getId()).thenReturn("1");
        Mockito.when(localView.getInstances()).thenReturn(Collections.singletonList(local));
        Mockito.when(view.getClusterViews()).thenReturn(Collections.singleton(localView));
        Mockito.when(local.getClusterView()).thenReturn(localView);

        return view;
    }

    private static class ChangeListener implements ConfigurationChangeListener {

        public final List<Boolean> events = new ArrayList<>();
        private volatile CountDownLatch latch;

        public void init(final int count) {
            events.clear();
            latch = new CountDownLatch(count);
        }

        public void await() throws Exception {
            if (!latch.await(8000, TimeUnit.MILLISECONDS)) {
                throw new Exception("No configuration event within 8 seconds.");
            }
        }

        @Override
        public void configurationChanged(boolean active) {
            events.add(active);
            latch.countDown();
        }
    }

    /**
     * Test that unbinding the readiness condition preserves topology and that
     * rebinding it restores listener active state — the core regression fix.
     */
    @Test
    public void testConditionTogglePreservesTopology() throws Exception {
        final ChangeListener ccl = new ChangeListener();
        final JobManagerConfiguration config = new JobManagerConfiguration();
        ((AtomicBoolean) TestUtil.getFieldValue(config, "active")).set(true);
        InitDelayingTopologyEventListener startupDelayListener =
                new InitDelayingTopologyEventListener(1, new TopologyEventListener() {
                    @Override
                    public void handleTopologyEvent(TopologyEvent event) {
                        config.doHandleTopologyEvent(event);
                    }
                });
        TestUtil.setFieldValue(config, "startupDelayListener", startupDelayListener);

        // Bind condition and register listener
        Condition condition = mock(Condition.class);
        config.bindJobProcessingEnabledCondition(condition);

        ccl.init(1);
        config.addListener(ccl);
        ccl.await();
        // No topology yet → active=false
        assertEquals(1, ccl.events.size());
        assertFalse(ccl.events.get(0));

        // Establish topology
        ccl.init(1);
        final TopologyView initView = createView();
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, initView));
        ccl.await();
        assertEquals(1, ccl.events.size());
        assertTrue("Listener should be active with topology + condition", ccl.events.get(0));

        // Unbind condition — topology must be preserved, listener gets active=false
        ccl.init(1);
        config.unbindJobProcessingEnabledCondition(condition);
        ccl.await();
        assertEquals(1, ccl.events.size());
        assertFalse("Listener should be inactive when condition is removed", ccl.events.get(0));
        assertFalse("isJobProcessingEnabled should be false", config.isJobProcessingEnabled());
        // Topology must still be present (this is the core fix)
        assertNotNull("Topology must be preserved after condition unbind", config.getTopologyCapabilities());

        // Rebind condition — listener should become active again without a topology event
        ccl.init(1);
        config.bindJobProcessingEnabledCondition(condition);
        ccl.await();
        assertEquals(1, ccl.events.size());
        assertTrue("Listener should be active again after condition rebind", ccl.events.get(0));
        assertTrue("isJobProcessingEnabled should be true", config.isJobProcessingEnabled());
    }

    /**
     * Test that addListener() sends the combined state (topology AND readiness),
     * not just topology-only. This covers the JobSchedulerImpl startup path.
     */
    @Test
    public void testAddListenerSendsCombinedState() throws Exception {
        final JobManagerConfiguration config = new JobManagerConfiguration();
        ((AtomicBoolean) TestUtil.getFieldValue(config, "active")).set(true);
        InitDelayingTopologyEventListener startupDelayListener =
                new InitDelayingTopologyEventListener(1, new TopologyEventListener() {
                    @Override
                    public void handleTopologyEvent(TopologyEvent event) {
                        config.doHandleTopologyEvent(event);
                    }
                });
        TestUtil.setFieldValue(config, "startupDelayListener", startupDelayListener);

        // Establish topology WITHOUT condition — use a helper listener to await async init
        final ChangeListener setupListener = new ChangeListener();
        setupListener.init(1);
        config.addListener(setupListener);
        setupListener.await(); // addListener sends initial state (false — no topology yet)

        final TopologyView initView = createView();
        setupListener.init(1);
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, initView));
        setupListener.await(); // topology is now established

        // Topology exists but condition is absent — addListener must send active=false
        final ChangeListener listener1 = new ChangeListener();
        listener1.init(1);
        config.addListener(listener1);
        listener1.await();
        assertEquals(1, listener1.events.size());
        assertFalse(
                "addListener must send false when topology exists but condition is absent", listener1.events.get(0));

        // Now bind condition — addListener must send active=true
        Condition condition = mock(Condition.class);
        config.bindJobProcessingEnabledCondition(condition);

        final ChangeListener listener2 = new ChangeListener();
        listener2.init(1);
        config.addListener(listener2);
        listener2.await();
        assertEquals(1, listener2.events.size());
        assertTrue("addListener must send true when both topology and condition are present", listener2.events.get(0));

        // Unbind condition — addListener must send active=false again
        config.unbindJobProcessingEnabledCondition(condition);

        final ChangeListener listener3 = new ChangeListener();
        listener3.init(1);
        config.addListener(listener3);
        listener3.await();
        assertEquals(1, listener3.events.size());
        assertFalse("addListener must send false after condition is unbound", listener3.events.get(0));
    }

    /**
     * Test rapid condition toggling with topology present — verifies that
     * the final state is consistent after multiple toggles.
     */
    @Test
    public void testRapidConditionToggle() throws Exception {
        final ChangeListener ccl = new ChangeListener();
        final JobManagerConfiguration config = new JobManagerConfiguration();
        ((AtomicBoolean) TestUtil.getFieldValue(config, "active")).set(true);
        InitDelayingTopologyEventListener startupDelayListener =
                new InitDelayingTopologyEventListener(1, new TopologyEventListener() {
                    @Override
                    public void handleTopologyEvent(TopologyEvent event) {
                        config.doHandleTopologyEvent(event);
                    }
                });
        TestUtil.setFieldValue(config, "startupDelayListener", startupDelayListener);

        // Bind condition, register listener (init latch before addListener), establish topology
        Condition condition = mock(Condition.class);
        config.bindJobProcessingEnabledCondition(condition);
        ccl.init(1);
        config.addListener(ccl);
        ccl.await(); // initial state (no topology yet → false)

        ccl.init(1);
        final TopologyView initView = createView();
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, initView));
        ccl.await(); // topology established → true

        // Rapid toggle 5 times — each fires notifyListeners, ccl absorbs them
        ccl.init(10); // 5 unbinds + 5 binds
        for (int i = 0; i < 5; i++) {
            config.unbindJobProcessingEnabledCondition(condition);
            config.bindJobProcessingEnabledCondition(condition);
        }
        ccl.await();

        // Final state: condition is bound, topology is preserved
        assertTrue("isJobProcessingEnabled should be true after toggles", config.isJobProcessingEnabled());
        assertNotNull("Topology must survive rapid toggles", config.getTopologyCapabilities());

        // Verify listener gets correct state on explicit toggles
        ccl.init(1);
        config.unbindJobProcessingEnabledCondition(condition);
        ccl.await();
        assertFalse("Last event should be false after final unbind", ccl.events.get(0));

        ccl.init(1);
        config.bindJobProcessingEnabledCondition(condition);
        ccl.await();
        assertTrue("Last event should be true after final rebind", ccl.events.get(0));
    }

    @Test
    public void testTopologyChange() throws Exception {
        // mock scheduler
        final ChangeListener ccl = new ChangeListener();

        // add change listener and verify
        ccl.init(1);
        final JobManagerConfiguration config = new JobManagerConfiguration();
        ((AtomicBoolean) TestUtil.getFieldValue(config, "active")).set(true);
        InitDelayingTopologyEventListener startupDelayListener =
                new InitDelayingTopologyEventListener(1, new TopologyEventListener() {

                    @Override
                    public void handleTopologyEvent(TopologyEvent event) {
                        config.doHandleTopologyEvent(event);
                    }
                });
        TestUtil.setFieldValue(config, "startupDelayListener", startupDelayListener);

        // Create and bind the condition
        Condition condition = mock(Condition.class);
        config.bindJobProcessingEnabledCondition(condition);

        config.addListener(ccl);
        ccl.await();

        assertEquals(1, ccl.events.size());
        assertFalse(ccl.events.get(0));

        // create init view
        ccl.init(1);
        final TopologyView initView = createView();
        final TopologyEvent init = new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, initView);
        config.handleTopologyEvent(init);
        ccl.await();

        assertEquals(1, ccl.events.size());
        assertTrue(ccl.events.get(0));

        // change view, followed by change props
        ccl.init(2);
        final TopologyView view2 = createView();
        Mockito.when(initView.isCurrent()).thenReturn(false);
        final TopologyEvent change1 = new TopologyEvent(TopologyEvent.Type.TOPOLOGY_CHANGED, initView, view2);
        final TopologyView view3 = createView();
        final TopologyEvent change2 = new TopologyEvent(TopologyEvent.Type.PROPERTIES_CHANGED, view2, view3);

        config.handleTopologyEvent(change1);
        Mockito.when(view2.isCurrent()).thenReturn(false);
        config.handleTopologyEvent(change2);

        ccl.await();
        assertEquals(2, ccl.events.size());
        assertFalse(ccl.events.get(0));
        assertTrue(ccl.events.get(1));

        // we wait another 4 secs to see if there is no another event
        Thread.sleep(4000);
        assertEquals(2, ccl.events.size());
    }
}
