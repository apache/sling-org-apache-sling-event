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
import java.util.List;
import java.util.concurrent.*;

/**
 * A ScheduledExecutorService that captures submitted tasks without executing them.
 * Models realistic shutdown semantics: {@link #shutdownNow()} marks the scheduler as shut down
 * and clears pending tasks; {@link #schedule} after shutdown throws {@link RejectedExecutionException};
 * {@link #runAll()} skips execution after shutdown.
 */
class ManualScheduler implements ScheduledExecutorService {

    private final List<Runnable> tasks = new ArrayList<>();
    private boolean shutdown;

    /**
     * Execute all captured tasks and clear the list. No-op after shutdown.
     */
    public void runAll() {
        if (shutdown) {
            return;
        }
        final List<Runnable> copy = new ArrayList<>(tasks);
        tasks.clear();
        copy.forEach(Runnable::run);
    }

    /**
     * Return the number of pending (un-executed) tasks.
     */
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
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
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

    /**
     * Minimal no-op ScheduledFuture returned by schedule().
     */
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
