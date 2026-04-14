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
import java.util.concurrent.Delayed;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
     * A ScheduledExecutorService that captures submitted tasks without executing them.
     * Models realistic shutdown semantics: {@link #shutdownNow()} marks the scheduler as shut down
     * and clears pending tasks; {@link #schedule} after shutdown throws {@link RejectedExecutionException};
     * {@link #runAll()} skips execution after shutdown.
     */
    private static class ManualScheduler implements ScheduledExecutorService {

        private final List<Runnable> tasks = new ArrayList<>();
        private boolean shutdown;

        /** Execute all captured tasks and clear the list. No-op after shutdown. */
        public void runAll() {
            if (shutdown) {
                return;
            }
            final List<Runnable> copy = new ArrayList<>(tasks);
            tasks.clear();
            copy.forEach(Runnable::run);
        }

        /** Return the number of pending (un-executed) tasks. */
        public int pendingCount() {
            return tasks.size();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            if (shutdown) {
                throw new RejectedExecutionException("Scheduler has been shut down");
            }
            tasks.add(command);
            return new CompletedFuture<>();
        }

        // -- unused ScheduledExecutorService methods --

        @Override
        public <V> ScheduledFuture<V> schedule(java.util.concurrent.Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            final List<Runnable> pending = new ArrayList<>(tasks);
            tasks.clear();
            return pending;
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public <T> java.util.concurrent.Future<T> submit(java.util.concurrent.Callable<T> task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> java.util.concurrent.Future<T> submit(Runnable task, T result) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.Future<?> submit(Runnable task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<java.util.concurrent.Future<T>> invokeAll(
                java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<java.util.concurrent.Future<T>> invokeAll(
                java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(
                java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException();
        }

        /** Minimal no-op ScheduledFuture returned by schedule(). */
        private static class CompletedFuture<V> implements ScheduledFuture<V> {
            @Override
            public long getDelay(TimeUnit unit) {
                return 0;
            }

            @Override
            public int compareTo(Delayed o) {
                return 0;
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return true;
            }

            @Override
            public V get() {
                return null;
            }

            @Override
            public V get(long timeout, TimeUnit unit) {
                return null;
            }
        }
    }

    private JobManagerConfiguration createConfig(ManualScheduler manualScheduler) {
        final JobManagerConfiguration config = new JobManagerConfiguration();
        ((AtomicBoolean) TestUtil.getFieldValue(config, "active")).set(true);
        config.setScheduler(manualScheduler);
        InitDelayingTopologyEventListener startupDelayListener =
                new InitDelayingTopologyEventListener(1, new TopologyEventListener() {

                    @Override
                    public void handleTopologyEvent(TopologyEvent event) {
                        config.doHandleTopologyEvent(event);
                    }
                });
        TestUtil.setFieldValue(config, "startupDelayListener", startupDelayListener);
        return config;
    }

    @Test
    public void testTopologyChange() throws Exception {
        final ChangeListener ccl = new ChangeListener();
        final ManualScheduler manualScheduler = new ManualScheduler();
        final JobManagerConfiguration config = createConfig(manualScheduler);

        // Create and bind the condition
        Condition condition = mock(Condition.class);
        config.bindJobProcessingEnabledCondition(condition);

        ccl.init(1);
        config.addListener(ccl);
        ccl.await();

        assertEquals(1, ccl.events.size());
        assertFalse(ccl.events.get(0));

        // TOPOLOGY_INIT notifies listeners synchronously (no scheduler involved)
        ccl.init(1);
        final TopologyView initView = createView();
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, initView));
        ccl.await();

        assertEquals(1, ccl.events.size());
        assertTrue(ccl.events.get(0));

        // TOPOLOGY_CHANGED: stopProcessing fires synchronous false, startProcessing schedules delayed task
        ccl.init(1);
        final TopologyView view2 = createView();
        Mockito.when(initView.isCurrent()).thenReturn(false);
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_CHANGED, initView, view2));
        ccl.await();

        assertEquals(1, ccl.events.size());
        assertFalse("stopProcessing should fire active=false", ccl.events.get(0));
        assertEquals("TOPOLOGY_CHANGED should schedule one delayed task", 1, manualScheduler.pendingCount());

        // PROPERTIES_CHANGED: for views with same capabilities, stopProcessing is skipped,
        // only a new delayed task is scheduled
        final TopologyView view3 = createView();
        Mockito.when(view2.isCurrent()).thenReturn(false);
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.PROPERTIES_CHANGED, view2, view3));

        assertEquals("Two delayed tasks pending", 2, manualScheduler.pendingCount());

        // Execute all pending tasks — only the latest should fire (earlier caps were deactivated)
        ccl.init(1);
        manualScheduler.runAll();
        ccl.await();

        assertEquals(1, ccl.events.size());
        assertTrue("Delayed task should notify active=true", ccl.events.get(0));
        assertEquals("No pending tasks remain", 0, manualScheduler.pendingCount());
    }

    /**
     * Verify that deactivation prevents delayed listener notifications from firing
     * and that a schedule/shutdown race does not produce exceptions.
     */
    @Test
    public void testDeactivatePreventsDelayedNotification() throws Exception {
        final ChangeListener ccl = new ChangeListener();
        final ManualScheduler manualScheduler = new ManualScheduler();
        final JobManagerConfiguration config = createConfig(manualScheduler);

        Condition condition = mock(Condition.class);
        config.bindJobProcessingEnabledCondition(condition);

        ccl.init(1);
        config.addListener(ccl);
        ccl.await(); // initial state: no topology → false

        // Establish topology via TOPOLOGY_INIT (synchronous notification)
        ccl.init(1);
        final TopologyView initView = createView();
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, initView));
        ccl.await();
        assertTrue("Should be active after TOPOLOGY_INIT", ccl.events.get(0));

        // TOPOLOGY_CHANGED queues a delayed task via the scheduler
        ccl.init(1);
        final TopologyView view2 = createView();
        Mockito.when(initView.isCurrent()).thenReturn(false);
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_CHANGED, initView, view2));
        ccl.await(); // synchronous false from stopProcessing
        assertEquals("One delayed task should be pending", 1, manualScheduler.pendingCount());

        // Deactivate before the delayed task runs — this calls shutdownNow() on the scheduler
        // (deactivate also calls stopProcessing → notifyListeners which fires a synchronous event)
        config.deactivate();
        ccl.events.clear();

        // shutdownNow() must have cleared the pending task
        assertEquals("Pending tasks must be cleared after deactivate", 0, manualScheduler.pendingCount());
        assertTrue("Scheduler must be shut down", manualScheduler.isShutdown());

        // Running the scheduler after shutdown must not deliver any events
        manualScheduler.runAll();
        assertTrue("No events should be delivered after deactivate", ccl.events.isEmpty());

        // Simulate the race: a topology event arrives while deactivation is in progress.
        // The production code reads scheduler.get() which now returns null (AtomicReference
        // was cleared by deactivate). This must not throw NPE.
        // To also exercise the RejectedExecutionException path, we re-inject the shut-down
        // scheduler and trigger a non-INIT event.
        config.setScheduler(manualScheduler);
        ((AtomicBoolean) TestUtil.getFieldValue(config, "active")).set(true);
        InitDelayingTopologyEventListener startupDelayListener =
                new InitDelayingTopologyEventListener(1, new TopologyEventListener() {

                    @Override
                    public void handleTopologyEvent(TopologyEvent event) {
                        config.doHandleTopologyEvent(event);
                    }
                });
        TestUtil.setFieldValue(config, "startupDelayListener", startupDelayListener);

        // This TOPOLOGY_INIT sets up topology state so the next TOPOLOGY_CHANGED reaches startProcessing
        final TopologyView view3 = createView();
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, view3));

        // Now trigger TOPOLOGY_CHANGED — scheduler.schedule() will throw RejectedExecutionException
        // which must be caught by the production code, not escape to the caller
        ccl.events.clear();
        final TopologyView view4 = createView();
        Mockito.when(view3.isCurrent()).thenReturn(false);
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_CHANGED, view3, view4));

        // No task should have been queued (scheduler rejected it)
        assertEquals("No tasks should be queued on shut-down scheduler", 0, manualScheduler.pendingCount());
    }

    /**
     * Verify that startProcessing() handles a null scheduler reference gracefully.
     * This covers the race where deactivate() has already cleared the AtomicReference
     * (via getAndSet(null)) before a topology event reaches startProcessing().
     */
    @Test
    public void testStartProcessingWithNullScheduler() throws Exception {
        final ChangeListener ccl = new ChangeListener();
        final ManualScheduler manualScheduler = new ManualScheduler();
        final JobManagerConfiguration config = createConfig(manualScheduler);

        Condition condition = mock(Condition.class);
        config.bindJobProcessingEnabledCondition(condition);

        ccl.init(1);
        config.addListener(ccl);
        ccl.await();

        // Establish topology via TOPOLOGY_INIT
        ccl.init(1);
        final TopologyView initView = createView();
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_INIT, null, initView));
        ccl.await();
        assertTrue(ccl.events.get(0));

        // Clear the scheduler reference directly — simulates deactivate() having already
        // called scheduler.getAndSet(null) before the topology event arrives
        config.setScheduler(null);

        // TOPOLOGY_CHANGED triggers stopProcessing (synchronous false) then startProcessing
        // which reads scheduler.get() → null and must return early without throwing NPE
        ccl.init(1);
        final TopologyView view2 = createView();
        Mockito.when(initView.isCurrent()).thenReturn(false);
        config.handleTopologyEvent(new TopologyEvent(TopologyEvent.Type.TOPOLOGY_CHANGED, initView, view2));
        ccl.await();

        // Only the synchronous false from stopProcessing should have been delivered
        assertEquals(1, ccl.events.size());
        assertFalse("Only stopProcessing event should fire", ccl.events.get(0));
        // No task queued anywhere — scheduler was null
        assertEquals("No tasks should be pending", 0, manualScheduler.pendingCount());
    }

    /**
     * Verify that deactivate() handles a null scheduler reference gracefully.
     * This covers the case where the scheduler was never initialized (e.g. activate()
     * was never called) or was already cleared by a prior deactivate().
     */
    @Test
    public void testDeactivateWithNullScheduler() {
        final JobManagerConfiguration config = new JobManagerConfiguration();
        ((AtomicBoolean) TestUtil.getFieldValue(config, "active")).set(true);
        // Do NOT set a scheduler — the AtomicReference holds null

        // deactivate() must not throw NPE when scheduler.getAndSet(null) returns null
        config.deactivate();

        assertFalse("Component should be inactive after deactivate", config.isActive());

        // Calling deactivate() a second time must also be safe (double-deactivate)
        config.deactivate();
        assertFalse("Component should remain inactive", config.isActive());
    }
}
