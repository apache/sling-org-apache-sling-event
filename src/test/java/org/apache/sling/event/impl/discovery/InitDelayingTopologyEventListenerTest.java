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
package org.apache.sling.event.impl.discovery;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.sling.discovery.ClusterView;
import org.apache.sling.discovery.InstanceDescription;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyEvent.Type;
import org.apache.sling.discovery.TopologyEventListener;
import org.apache.sling.discovery.TopologyView;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class InitDelayingTopologyEventListenerTest {

    class TestListener implements TopologyEventListener {

        private List<TopologyEvent> events = new LinkedList<TopologyEvent>();

        @Override
        public void handleTopologyEvent(TopologyEvent event) {
            synchronized (events) {
                events.add(event);
                events.notifyAll();
            }
        }

        public List<TopologyEvent> getEvents() {
            synchronized (events) {
                return events;
            }
        }

        public void waitForEventCnt(int cnt, long timeout) throws InterruptedException {
            final long start = System.currentTimeMillis();
            synchronized (events) {
                while (events.size() != cnt) {
                    final long now = System.currentTimeMillis();
                    final long remaining = (start + timeout) - now;
                    if (remaining > 0) {
                        events.wait(remaining);
                    } else {
                        fail("did not receive " + cnt + " events within " + timeout + " ms, "
                            + "but " + events.size());
                    }
                }
            }
        }

        public void assureEventCnt(int cnt, int timeout) throws InterruptedException {
            final long start = System.currentTimeMillis();
            synchronized (events) {
                while (events.size() == cnt) {
                    final long now = System.currentTimeMillis();
                    final long remaining = (start + timeout) - now;
                    if (remaining > 0) {
                        events.wait(remaining);
                    } else {
                        // success
                        return;
                    }
                }
                fail("did not receive " + cnt + " events within " + timeout + " ms, "
                    + "but " + events.size());
            }
        }
    }

    @Test
    public void testConstructor() throws Exception {
        final TopologyEventListener delegate = new TopologyEventListener() {

            @Override
            public void handleTopologyEvent(TopologyEvent event) {
                // nothing here atm
            }
        };
        try {
            new InitDelayingTopologyEventListener(-1, delegate);
            fail("should complain");
        } catch (IllegalArgumentException re) {
            // ok
        }
        try {
            new InitDelayingTopologyEventListener(0, delegate);
            fail("should complain");
        } catch (IllegalArgumentException re) {
            // ok
        }
        try {
            new InitDelayingTopologyEventListener(1, null);
            fail("should complain");
        } catch (IllegalArgumentException re) {
            // ok
        }
        try {
            new InitDelayingTopologyEventListener(-1, delegate);
            fail("should complain");
        } catch (IllegalArgumentException re) {
            // ok
        }
        try {
            new InitDelayingTopologyEventListener(0, delegate);
            fail("should complain");
        } catch (IllegalArgumentException re) {
            // ok
        }
        try {
            new InitDelayingTopologyEventListener(1, null);
            fail("should complain");
        } catch (IllegalArgumentException re) {
            // ok
        }
        try {
            new InitDelayingTopologyEventListener(-1, delegate);
            fail("should complain");
        } catch (IllegalArgumentException re) {
            // ok
        }
        try {
            new InitDelayingTopologyEventListener(0, delegate);
            fail("should complain");
        } catch (IllegalArgumentException re) {
            // ok
        }
        try {
            new InitDelayingTopologyEventListener(1, null);
            fail("should complain");
        } catch (IllegalArgumentException re) {
            // ok
        }
    }

    private TopologyView createView(boolean current) {
        final TopologyView view = Mockito.mock(TopologyView.class);
        Mockito.when(view.isCurrent()).thenReturn(current);
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

    private TopologyEvent createEvent(Type type) {
        TopologyView oldView = createView(false);
        TopologyView newView = createView(true);
        switch (type) {
            case TOPOLOGY_CHANGING: {
                return new TopologyEvent(type, oldView, null);
            }
            case PROPERTIES_CHANGED:
            case TOPOLOGY_CHANGED: {
                return new TopologyEvent(type, oldView, newView);
            }
            case TOPOLOGY_INIT: {
                return new TopologyEvent(type, null, newView);
            }
            default: {
                throw new IllegalArgumentException("unknown type: " + type);
            }
        }
    }

    @Test
    public void testDisposing() throws Exception {
        final TestListener delegate = new TestListener();
        InitDelayingTopologyEventListener listener = new InitDelayingTopologyEventListener(1, delegate);
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_INIT));
        delegate.waitForEventCnt(1, 5000);
        delegate.assureEventCnt(1, 500); // test framework testing :)

        listener = new InitDelayingTopologyEventListener(1, delegate);
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_INIT));
        listener.dispose();
        delegate.assureEventCnt(1, 1000);
        delegate.assureEventCnt(1, 500);
    }

    @Test
    public void testNoEvents() throws Exception {
        final TestListener delegate = new TestListener();
        InitDelayingTopologyEventListener listener = new InitDelayingTopologyEventListener(1, delegate);
        // no events:
        delegate.assureEventCnt(0, 1500);

        // then the first init is passed through
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_INIT));
        delegate.waitForEventCnt(1, 5000);
        assertEquals(delegate.getEvents().get(0).getType(), Type.TOPOLOGY_INIT);

        doTestAdditionalEventsAfterInit(delegate, listener);
    }

    private void doTestAdditionalEventsAfterInit(final TestListener delegate, InitDelayingTopologyEventListener listener)
        throws InterruptedException {
        // 2nd one too
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGING));
        delegate.waitForEventCnt(2, 5000);
        assertEquals(delegate.getEvents().get(1).getType(), Type.TOPOLOGY_CHANGING);

        // 3rd one too
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGED));
        delegate.waitForEventCnt(3, 5000);
        assertEquals(delegate.getEvents().get(2).getType(), Type.TOPOLOGY_CHANGED);

        // 4th one too
        listener.handleTopologyEvent(createEvent(Type.PROPERTIES_CHANGED));
        delegate.waitForEventCnt(4, 5000);
        assertEquals(delegate.getEvents().get(3).getType(), Type.PROPERTIES_CHANGED);

        // 5th one too
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGING));
        delegate.waitForEventCnt(5, 5000);
        assertEquals(delegate.getEvents().get(4).getType(), Type.TOPOLOGY_CHANGING);

        // 6th one too
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGED));
        delegate.waitForEventCnt(6, 5000);
        assertEquals(delegate.getEvents().get(5).getType(), Type.TOPOLOGY_CHANGED);

    }

    @Test
    public void testChanging0() throws Exception {
        final TestListener delegate = new TestListener();
        InitDelayingTopologyEventListener listener = new InitDelayingTopologyEventListener(1, delegate);
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_INIT));
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGING));
        delegate.assureEventCnt(0, 1000);
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGED));
        delegate.waitForEventCnt(1, 5000);
        assertEquals(delegate.getEvents().get(0).getType(), Type.TOPOLOGY_INIT);

        doTestAdditionalEventsAfterInit(delegate, listener);
    }

    @Test
    public void testChanging1() throws Exception {
        final TestListener delegate = new TestListener();
        InitDelayingTopologyEventListener listener = new InitDelayingTopologyEventListener(1, delegate);
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_INIT));
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGING));
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGED));
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGING));
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGED));
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGING));
        delegate.assureEventCnt(0, 1000);
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGED));
        delegate.waitForEventCnt(1, 5000);
        assertEquals(delegate.getEvents().get(0).getType(), Type.TOPOLOGY_INIT);

        doTestAdditionalEventsAfterInit(delegate, listener);
    }

    @Test
    public void testChanged() throws Exception {
        final TestListener delegate = new TestListener();
        InitDelayingTopologyEventListener listener = new InitDelayingTopologyEventListener(1, delegate);
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_INIT));
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGING));
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGED));
        delegate.waitForEventCnt(1, 5000);
        assertEquals(delegate.getEvents().get(0).getType(), Type.TOPOLOGY_INIT);

        doTestAdditionalEventsAfterInit(delegate, listener);
    }

    @Ignore
    @Test
    public void testProperties() throws Exception {
        final TestListener delegate = new TestListener();
        InitDelayingTopologyEventListener listener = new InitDelayingTopologyEventListener(2, delegate);
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_INIT));
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGING));
        listener.handleTopologyEvent(createEvent(Type.TOPOLOGY_CHANGED));
        listener.handleTopologyEvent(createEvent(Type.PROPERTIES_CHANGED));
        delegate.waitForEventCnt(1, 10000);
        assertEquals(delegate.getEvents().get(0).getType(), Type.TOPOLOGY_INIT);

        doTestAdditionalEventsAfterInit(delegate, listener);
    }

}
