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
package org.apache.sling.event.impl.jobs.stats;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.sling.event.jobs.Statistics;

/**
 * Implementation of the statistics.
 */
public class StatisticsImpl extends BaseStatisticsImpl implements Statistics {

    private final AtomicLong startTime = new AtomicLong();

    private final AtomicLong activeJobs = new AtomicLong();

    private final AtomicLong queuedJobs = new AtomicLong();

    public StatisticsImpl() {
        this(System.currentTimeMillis());
    }

    public StatisticsImpl(final long startTime) {
        this.startTime.set(startTime);
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#getStartTime()
     */
    @Override
    public long getStartTime() {
        return startTime.get();
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#getNumberOfActiveJobs()
     */
    @Override
    public long getNumberOfActiveJobs() {
        return activeJobs.get();
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#getNumberOfQueuedJobs()
     */
    @Override
    public long getNumberOfQueuedJobs() {
        return queuedJobs.get();
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#getNumberOfJobs()
     */
    @Override
    public long getNumberOfJobs() {
        return activeJobs.get() + queuedJobs.get();
    }

    /**
     * Add a finished job
     * @param jobTime The processing time for this job.
     */
    public synchronized void finishedJob(final long jobTime) {
        super.finishedJob(jobTime);
        if (this.activeJobs.get() > 0) {
            this.activeJobs.decrementAndGet();
        }
    }

    /**
     * Add a failed job.
     */
    public synchronized void failedJob() {
        super.failedJob();
        if (this.activeJobs.get() > 0) {
            this.activeJobs.decrementAndGet();
        }
    }

    /**
     * Add a cancelled job.
     */
    public synchronized void cancelledJob() {
        super.cancelledJob();
        if (this.activeJobs.get() > 0) {
            this.activeJobs.decrementAndGet();
        }
    }

    /**
     * New job in the queue
     */
    public void incQueued() {
        this.queuedJobs.incrementAndGet();
    }

    /**
     * Job not processed by us
     */
    public void decQueued() {
        if (this.queuedJobs.get() > 0) {
            this.queuedJobs.decrementAndGet();
        }
    }

    /**
     * Clear all queued
     */
    public synchronized void clearQueued() {
        this.queuedJobs.set(0);
    }

    /**
     * Add a job from the queue to status active
     * @param queueTime The time the job stayed in the queue.
     */
    public synchronized void addActive(final long queueTime) {
        super.addActive(queueTime);
        if (this.queuedJobs.get() > 0) {
            this.queuedJobs.decrementAndGet();
        }
        this.activeJobs.incrementAndGet();
    }

    /**
     * Add another statistics information.
     */
    public synchronized void add(final StatisticsImpl other) {
        synchronized ( other ) {
            super.add(other);
            this.queuedJobs.addAndGet(other.queuedJobs.get());
            this.activeJobs.addAndGet(other.activeJobs.get());
        }
    }

    /**
     * Create a new statistics object with exactly the same values.
     */
    public void copyFrom(final StatisticsImpl other) {
        super.copyFrom(other);
        final long localQueuedJobs;
        final long localActiveJobs;
        synchronized ( other ) {
            localQueuedJobs = other.queuedJobs.get();
            localActiveJobs = other.activeJobs.get();
        }
        synchronized ( this ) {
            this.queuedJobs.set(localQueuedJobs);
            this.activeJobs.set(localActiveJobs);
        }
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#reset()
     */
    @Override
    public synchronized void reset() {
        this.startTime.set(System.currentTimeMillis());
        this.activeJobs.set(0);
        this.queuedJobs.set(0);
        super.reset();
    }
}
