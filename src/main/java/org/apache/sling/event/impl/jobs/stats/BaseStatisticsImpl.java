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

/**
 * Base class for statistics implementations
 */
public abstract class BaseStatisticsImpl {

    private final AtomicLong lastActivated = new AtomicLong(-1);

    private final AtomicLong lastFinished = new AtomicLong(-1);

    private final AtomicLong waitingTime = new AtomicLong();

    private final AtomicLong processingTime = new AtomicLong();

    private final AtomicLong waitingCount = new AtomicLong();

    private final AtomicLong processingCount = new AtomicLong();

    private final AtomicLong finishedJobs = new AtomicLong();

    private final AtomicLong failedJobs = new AtomicLong();

    private final AtomicLong cancelledJobs = new AtomicLong();

    /**
     * @see org.apache.sling.event.jobs.Statistics#getNumberOfProcessedJobs()
     */
    public long getNumberOfProcessedJobs() {
        return finishedJobs.get() + failedJobs.get() + cancelledJobs.get();
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#getAverageWaitingTime()
     */
    public long getAverageWaitingTime() {
        final long time = this.waitingTime.get();
        final long count = this.waitingCount.get();
        if (count > 1) {
            return time / count;
        }
        return time;
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#getAverageProcessingTime()
     */
    public long getAverageProcessingTime() {
        final long time = this.processingTime.get();
        final long count = this.processingCount.get();
        if (count > 1) {
            return time / count;
        }
        return time;
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#getNumberOfFinishedJobs()
     */
    public long getNumberOfFinishedJobs() {
        return finishedJobs.get();
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#getNumberOfCancelledJobs()
     */
    public long getNumberOfCancelledJobs() {
        return cancelledJobs.get();
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#getNumberOfFailedJobs()
     */
    public long getNumberOfFailedJobs() {
        return failedJobs.get();
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#getLastActivatedJobTime()
     */
    public long getLastActivatedJobTime() {
        return lastActivated.get();
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#getLastFinishedJobTime()
     */
    public long getLastFinishedJobTime() {
        return lastFinished.get();
    }

    /**
     * Add a finished job
     * @param jobTime The processing time for this job.
     */
    public synchronized void finishedJob(final long jobTime) {
        this.lastFinished.set(System.currentTimeMillis());
        this.processingTime.addAndGet(jobTime);
        this.processingCount.incrementAndGet();
        this.finishedJobs.incrementAndGet();
    }

    /**
     * Add a job from the queue to status active
     * @param queueTime The time the job stayed in the queue.
     */
    public synchronized void addActive(final long queueTime) {
        this.waitingCount.incrementAndGet();
        this.waitingTime.addAndGet(queueTime);
        this.lastActivated.set(System.currentTimeMillis());
    }

    /**
     * Add a failed job.
     */
    public synchronized void failedJob() {
        this.failedJobs.incrementAndGet();
    }

    /**
     * Add a cancelled job.
     */
    public synchronized void cancelledJob() {
        this.cancelledJobs.incrementAndGet();
    }

    /**
     * Add another statistics information.
     */
    public synchronized void add(final BaseStatisticsImpl other) {
        synchronized (other) {
            if (other.lastActivated.get() > this.lastActivated.get()) {
                this.lastActivated.set(other.lastActivated.get());
            }
            if (other.lastFinished.get() > this.lastFinished.get()) {
                this.lastFinished.set(other.lastFinished.get());
            }
            this.waitingTime.addAndGet(other.waitingTime.get());
            this.waitingCount.addAndGet(other.waitingCount.get());
            this.processingTime.addAndGet(other.processingTime.get());
            this.processingCount.addAndGet(other.processingCount.get());
            this.finishedJobs.addAndGet(other.finishedJobs.get());
            this.failedJobs.addAndGet(other.failedJobs.get());
            this.cancelledJobs.addAndGet(other.cancelledJobs.get());
        }
    }

    /**
     * Create a new statistics object with exactly the same values.
     */
    public void copyFrom(final BaseStatisticsImpl other) {
        final long localLastActivated;
        final long localLastFinished;
        final long localWaitingTime;
        final long localProcessingTime;
        final long localWaitingCount;
        final long localProcessingCount;
        final long localFinishedJobs;
        final long localFailedJobs;
        final long localCancelledJobs;
        synchronized (other) {
            localLastActivated = other.lastActivated.get();
            localLastFinished = other.lastFinished.get();
            localWaitingTime = other.waitingTime.get();
            localProcessingTime = other.processingTime.get();
            localWaitingCount = other.waitingCount.get();
            localProcessingCount = other.processingCount.get();
            localFinishedJobs = other.finishedJobs.get();
            localFailedJobs = other.failedJobs.get();
            localCancelledJobs = other.cancelledJobs.get();
        }
        synchronized (this) {
            this.lastActivated.set(localLastActivated);
            this.lastFinished.set(localLastFinished);
            this.waitingTime.set(localWaitingTime);
            this.processingTime.set(localProcessingTime);
            this.waitingCount.set(localWaitingCount);
            this.processingCount.set(localProcessingCount);
            this.finishedJobs.set(localFinishedJobs);
            this.failedJobs.set(localFailedJobs);
            this.cancelledJobs.set(localCancelledJobs);
        }
    }

    /**
     * @see org.apache.sling.event.jobs.Statistics#reset()
     */
    public synchronized void reset() {
        this.lastActivated.set(-1);
        this.lastFinished.set(-1);
        this.waitingTime.set(0);
        this.processingTime.set(0);
        this.waitingCount.set(0);
        this.processingCount.set(0);
        this.finishedJobs.set(0);
        this.failedJobs.set(0);
        this.cancelledJobs.set(0);
    }
}
