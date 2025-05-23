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
package org.apache.sling.event.impl.jobs.queues;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.commons.threads.ThreadPool;
import org.apache.sling.event.impl.EventingThreadPool;
import org.apache.sling.event.impl.jobs.InternalJobState;
import org.apache.sling.event.impl.jobs.JobHandler;
import org.apache.sling.event.impl.jobs.JobImpl;
import org.apache.sling.event.impl.jobs.JobTopicTraverser;
import org.apache.sling.event.impl.jobs.Utility;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration;
import org.apache.sling.event.impl.jobs.notifications.NotificationUtility;
import org.apache.sling.event.impl.support.BatchResourceRemover;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.Job.JobState;
import org.apache.sling.event.jobs.NotificationConstants;
import org.apache.sling.event.jobs.Queue;
import org.apache.sling.event.jobs.QueueConfiguration.Type;
import org.apache.sling.event.jobs.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The job blocking queue extends the blocking queue by some
 * functionality for the job event handling.
 */
public class JobQueueImpl
    implements Queue {

    /** Default timeout for suspend. */
    private static final long MAX_SUSPEND_TIME = 1000 * 60 * 60; // 60 mins

    /** The logger. */
    private final Logger logger;

    /** Configuration. */
    private final InternalQueueConfiguration configuration;

    /** The queue name. */
    private volatile String queueName;

    /** Are we still running? */
    private volatile boolean running;

    /** Suspended since. */
    private final AtomicLong suspendedSince = new AtomicLong(-1);

    /** Services used by the queues. */
    private final QueueServices services;

    /** The map of events we're processing. */
    private final Map<String, JobHandler> processingJobsLists = new HashMap<>();

    private final ThreadPool threadPool;

    /** Async counter. */
    private final AtomicInteger asyncCounter = new AtomicInteger();

    /** Flag for outdated. */
    private final AtomicBoolean isOutdated = new AtomicBoolean(false);

    /** A marker for closing the queue. */
    private final AtomicBoolean closeMarker = new AtomicBoolean(false);

    /** A marker for doing a full cache search. */
    private final AtomicBoolean doFullCacheSearch = new AtomicBoolean(false);

    /** A counter for rescheduling. */
    private final AtomicInteger waitCounter = new AtomicInteger();

    /** The job cache. */
    private final QueueJobCache cache;

    /** Semaphore for handling the max number of jobs. */
    private final Semaphore available;

    /** original value of maxParallel with which the queue was created */
    private final int maxParallel;

    /** Semaphore for handling reduced number of available slots that are yet to drain */
    private final Semaphore drainage;

    /** Guard for having only one thread executing start jobs. */
    private final AtomicBoolean startJobsGuard = new AtomicBoolean(false);

    /** Lock for close/start. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Sleeping until is only set for ordered queues if a job is rescheduled. */
    private volatile long isSleepingUntil = -1;

    /**
     * Create a new queue.
     *
     * @param name The queue name
     * @param config The queue configuration
     * @param services The queue services
     * @param topics The topics handled by this queue
     * @param outdatedQueueInfo
     * @param haltedTopicsBackRef reference to pass newly halted topics back
     *
     * @return {@code JobQueueImpl} if there are jobs to process, {@code null} otherwise.
     */
    public static JobQueueImpl createQueue(final String name,
                        final InternalQueueConfiguration config,
                        final QueueServices services,
                        final Set<String> topics,
                        final Set<String> haltedTopicsBackRef,
                        final OutdatedJobQueueInfo outdatedQueueInfo) {
        final QueueJobCache cache = new QueueJobCache(services.configuration, name, services.statisticsManager, config.getType(), topics);
        // the cache might contain newly halted topics.
        // these we need to retain, to avoid scanning them going forward.
        // but since the cache might be empty and thus discarded,
        // we pass them back explicitly in provided haltedTopicsRef
        if ( !cache.getNewlyHaltedTopics().isEmpty() ) {
            for (String haltedTopic : cache.getNewlyHaltedTopics() ) {
                if (haltedTopicsBackRef.add(haltedTopic)) {
                    LoggerFactory.getLogger(JobQueueImpl.class.getName() + '.' + name)
                            .warn("createQueue : topic halted due to ClassNotFoundExceptions : "
                                    + haltedTopic);
                }
            }
        }
        if ( cache.isEmpty() ) {
            return null;
        }
        return new JobQueueImpl(name, config, services, cache, outdatedQueueInfo);
    }

    /**
     * Create a new queue.
     *
     * @param name The queue name
     * @param config The queue configuration
     * @param services The queue services
     * @param cache The job cache
     * @param outdatedQueue
     */
    protected JobQueueImpl(final String name,
                        final InternalQueueConfiguration config,
                        final QueueServices services,
                        final QueueJobCache cache,
                        final OutdatedJobQueueInfo outdatedQueue) {
        if ( config.getOwnThreadPoolSize() > 0 ) {
            this.threadPool = new EventingThreadPool(services.threadPoolManager, config.getOwnThreadPoolSize());
        } else {
            this.threadPool = services.eventingThreadPool;
        }
        this.queueName = name;
        this.configuration = config;
        this.services = services;
        this.logger = LoggerFactory.getLogger(this.getClass().getName() + '.' + name);
        this.running = true;
        this.cache = cache;
        this.maxParallel = config.getMaxParallel();
        if (outdatedQueue == null) {
            // queue is created the first time
            this.available = new Semaphore(this.maxParallel, true);
            this.drainage = new Semaphore(0, true);
        } else {
            // queue was previously outdated - let's reuse available and drainage
            this.available = outdatedQueue.getAvailable();
            this.drainage = outdatedQueue.getDrainage();
            int oldMaxParallel = outdatedQueue.getMaxParallel();
            int maxParallelDiff = this.maxParallel - oldMaxParallel;
            int drainedOldDrainage = 0;
            int drainedOldAvailable = 0;
            if (maxParallelDiff != 0) {
                // config change
                drainedOldDrainage = this.drainage.drainPermits();
                drainedOldAvailable = this.available.drainPermits();
                int netNewPermits = drainedOldAvailable - drainedOldDrainage + maxParallelDiff;
                if (netNewPermits > 0) {
                    this.available.release(netNewPermits);
                } else if (netNewPermits < 0) {
                    // special case : maxparallel got reduced since last outdating,
                    // resulting in effectively negative number of currently available permits.
                    // to account for that, jobs try to drain first before re-adding to available
                    // to trigger this behaviour, releasing the permit-diff to drainage
                    this.drainage.release(-netNewPermits);
                }
            }
            logger.info("<init> reused outdated queue info: queueName : {}"
                    + ", old available : {}, old drainage : {}, old maxParallel : {}"
                    + ", new available : {}, new drainage : {}, new maxParallel : {}",
                    queueName, drainedOldAvailable, drainedOldDrainage, oldMaxParallel,
                    available.availablePermits(), drainage.availablePermits(), this.maxParallel);
        }
        logger.info("Starting job queue {}", queueName);
        logger.debug("Configuration for job queue={}", configuration);
    }

    /**
     * Return the queue configuration
     */
    @Override
    public InternalQueueConfiguration getConfiguration() {
        return this.configuration;
    }

    /**
     * Get the name of the job queue.
     */
    @Override
    public String getName() {
        return this.queueName;
    }

    /**
     * @see org.apache.sling.event.jobs.Queue#getStatistics()
     */
    @Override
    public Statistics getStatistics() {
        return this.services.statisticsManager.getQueueStatistics(this.queueName);
    }

    /**
     * Start the job queue.
     * This method might be called concurrently, therefore we use a guard
     */
    public void startJobs() {
        if (!services.configuration.isJobProcessingEnabled()) {
            logger.debug("Job processing is disabled, skipping job starts for queue {}", queueName);
            return;
        }
        if ( this.startJobsGuard.compareAndSet(false, true) ) {
            // we start as many jobs in parallel as possible
            while ( this.running && !this.isOutdated.get() && !this.isSuspended() && this.available.tryAcquire() ) {
                boolean started = false;
                this.lock.writeLock().lock();
                try {
                    final JobHandler handler = this.cache.getNextJob(this.services.jobConsumerManager,
                            this.services.statisticsManager, this, this.doFullCacheSearch.getAndSet(false));
                    if ( handler != null ) {
                        started = true;
                        this.threadPool.execute(new Runnable() {

                            @Override
                            public void run() {
                                // update thread priority and name
                                final Thread currentThread = Thread.currentThread();
                                final String oldName = currentThread.getName();
                                final int oldPriority = currentThread.getPriority();

                                currentThread.setName(oldName + "-" + handler.getJob().getQueueName() + "(" + handler.getJob().getTopic() + ")");
                                if ( configuration.getThreadPriority() != null ) {
                                    switch ( configuration.getThreadPriority() ) {
                                        case NORM : currentThread.setPriority(Thread.NORM_PRIORITY);
                                                    break;
                                        case MIN  : currentThread.setPriority(Thread.MIN_PRIORITY);
                                                    break;
                                        case MAX  : currentThread.setPriority(Thread.MAX_PRIORITY);
                                                    break;
                                    }
                                }

                                try {
                                    startJob(handler);
                                } finally {
                                    currentThread.setPriority(oldPriority);
                                    currentThread.setName(oldName);
                                }
                                // and try to launch another job
                                startJobs();
                            }
                        });
                    } else {
                        // no job available, stop look
                        break;
                    }

                } finally {
                    if ( !started ) {
                        this.available.release();
                    }
                    this.lock.writeLock().unlock();
                }
            }
            this.startJobsGuard.set(false);
        }
    }

    private void startJob(final JobHandler handler) {
        if (!services.configuration.isJobProcessingEnabled()) {
            logger.debug("Job processing is disabled, stopping job {} in queue {}", handler.getJob().getId(), queueName);
            return;
        }
        try {
            this.closeMarker.set(false);
            try {
                final JobImpl job = handler.getJob();
                handler.started = System.currentTimeMillis();

                this.services.configuration.getAuditLogger().debug("START OK : {}", job.getId());
                // sanity check for the queued property
                Calendar queued = job.getProperty(JobImpl.PROPERTY_JOB_QUEUED, Calendar.class);
                if ( queued == null ) {
                    // we simply use a date of ten seconds ago
                    queued = Calendar.getInstance();
                    queued.setTimeInMillis(System.currentTimeMillis() - 10000);
                }
                final long queueTime = handler.started - queued.getTimeInMillis();
                // update statistics
                this.services.statisticsManager.jobStarted(this.queueName, job.getTopic(), queueTime);
                // send notification
                NotificationUtility.sendNotification(this.services.eventAdmin, NotificationConstants.TOPIC_JOB_STARTED, job, queueTime);

                synchronized ( this.processingJobsLists ) {
                    this.processingJobsLists.put(job.getId(), handler);
                }

                JobExecutionResultImpl result = JobExecutionResultImpl.CANCELLED;
                Job.JobState resultState = Job.JobState.ERROR;
                final JobExecutionContextImpl ctx = new JobExecutionContextImpl(handler, new JobExecutionContextImpl.ASyncHandler() {

                    @Override
                    public void finished(final JobState state) {
                        services.jobConsumerManager.unregisterListener(job.getId());
                        finishedJob(job.getId(), state, true);
                        asyncCounter.decrementAndGet();
                    }
                });

                try {
                    synchronized ( ctx ) {
                        result = (JobExecutionResultImpl)handler.getConsumer().process(job, ctx);
                        if ( result == null ) { // ASYNC processing
                            services.jobConsumerManager.registerListener(job.getId(), handler.getConsumer(), ctx);
                            asyncCounter.incrementAndGet();
                            ctx.markAsync();
                        } else {
                            if ( result.succeeded() ) {
                                resultState = Job.JobState.SUCCEEDED;
                            } else if ( result.failed() ) {
                                resultState = Job.JobState.QUEUED;
                            } else if ( result.cancelled() ) {
                                if ( handler.isStopped() ) {
                                    resultState = Job.JobState.STOPPED;
                                } else {
                                    resultState = Job.JobState.ERROR;
                                }
                            }
                        }
                    }
                } catch (final Throwable t) { //NOSONAR
                    logger.error("Unhandled error occured in job processor " + t.getMessage() + " while processing job " + Utility.toString(job), t);
                    // we don't reschedule if an exception occurs
                    result = JobExecutionResultImpl.CANCELLED;
                    resultState = Job.JobState.ERROR;
                } finally {
                    if ( result != null ) {
                        if ( result.getRetryDelayInMs() != null ) {
                            job.setProperty(JobImpl.PROPERTY_DELAY_OVERRIDE, result.getRetryDelayInMs());
                        }
                        if ( result.getMessage() != null ) {
                           job.setProperty(Job.PROPERTY_RESULT_MESSAGE, result.getMessage());
                        }
                        this.finishedJob(job.getId(), resultState, false);
                    }
                }
            } catch (final Exception re) {
                // if an exception occurs, we just log
                this.logger.error("Exception during job processing.", re);
            }
        } finally {
            // try draining first
            if (this.drainage.tryAcquire()) {
                // special case : if drainage is used, this means maxparallel
                // got reconfigured and we are not releasing a permit to
                // available here, but instead reduce drainage.
                final int approxPermits = this.drainage.availablePermits();
                this.logger.debug("startJobHandler: drained 1 permit for {}, approx left to drain: {}",
                        queueName, approxPermits);
            } else {
                // otherwise release as usual
                this.available.release();
            }
        }
    }

    /**
     * Outdate this queue.
     */
    public void outdate() {
        if ( this.isOutdated.compareAndSet(false, true) ) {
            final String name = this.getName() + "<outdated>(" + this.hashCode() + ")";
            this.logger.info("Outdating queue {}, renaming to {}.", this.queueName, name);
            this.queueName = name;
        }
    }

    /**
     * Check if the queue can be closed
     */
    public boolean tryToClose() {
        // resume the queue as we want to close it!
        this.resume();
        this.lock.writeLock().lock();
        try {
            // check if possible
            if ( this.canBeClosed() ) {
                if ( this.closeMarker.get() ) {
                    this.close();
                    return true;
                }
                this.closeMarker.set(true);
            }
        } finally {
            this.lock.writeLock().unlock();
        }
        return false;
    }

    /**
     * Check whether this queue can be closed
     */
    private boolean canBeClosed() {
        return !this.isSuspended()
            && this.asyncCounter.get() == 0
            && this.waitCounter.get() == 0
            && this.available.availablePermits() == this.configuration.getMaxParallel();
    }

    /**
     * Close this queue.
     */
    public void close() {
        this.running = false;
        this.logger.debug("Shutting down job queue {}", queueName);
        this.resume();

        synchronized ( this.processingJobsLists ) {
            this.processingJobsLists.clear();
        }
        if ( this.configuration.getOwnThreadPoolSize() > 0 ) {
            ((EventingThreadPool)this.threadPool).release();
        }

        this.logger.info("Stopped job queue {}", this.queueName);
    }

    /**
     * Periodic maintenance
     */
    public void maintain() {
        if (!services.configuration.isJobProcessingEnabled()) {
            logger.debug("Job processing is disabled, skipping maintenance for queue {}", queueName);
            return;
        }
        // check suspended
        final long since = this.suspendedSince.get();
        if ( since != -1 && since + MAX_SUSPEND_TIME < System.currentTimeMillis() ) {
            logger.info("Waking up suspended queue. It has been suspended for more than {}ms", MAX_SUSPEND_TIME);
            this.resume();
        }

        // set full cache search
        this.doFullCacheSearch.set(true);

        this.startJobs();
    }

    /**
     * Inform the queue about new job for the given topics.
     * @param topics the new topics
     */
    public void wakeUpQueue(final Set<String> topics) {
        this.cache.handleNewTopics(topics);
    }

    /**
     * Put a job back in the queue
     * @param handler The job handler
     */
    private void requeue(final JobHandler handler) {
        this.cache.reschedule(this.queueName, handler, this.services.statisticsManager);
        this.startJobs();
    }

    private static final class RescheduleInfo {
        public boolean      reschedule = false;
        // processing time is only set of state is SUCCEEDED
        public long         processingTime;
        public Job.JobState state;
        public InternalJobState       finalState;
    }

    private RescheduleInfo handleReschedule(final JobHandler handler, final Job.JobState resultState) {
        final RescheduleInfo info = new RescheduleInfo();
        info.state = resultState;
        switch ( resultState ) {
            case SUCCEEDED : // job is finished
                if ( this.logger.isDebugEnabled() ) {
                    this.logger.debug("Finished job {}", Utility.toString(handler.getJob()));
                }
                info.processingTime = System.currentTimeMillis() - handler.started;
                info.finalState = InternalJobState.SUCCEEDED;
                break;
            case QUEUED : // check if we exceeded the number of retries
                final int retries = handler.getJob().getProperty(Job.PROPERTY_JOB_RETRIES, 0);
                int retryCount = handler.getJob().getProperty(Job.PROPERTY_JOB_RETRY_COUNT, 0);

                retryCount++;
                if ( retries != -1 && retryCount > retries ) {
                    if ( this.logger.isDebugEnabled() ) {
                        this.logger.error("Cancelled job {} after {} unsuccessful retries",
                                Utility.toString(handler.getJob()),
                                retries);
                    }
                    info.finalState = InternalJobState.CANCELLED;
                } else {
                    info.reschedule = true;
                    handler.getJob().retry();
                    this.logger.warn("Failed job {}, will retry {} more time(s), retryCount={}",
                            Utility.toString(handler.getJob()),
                            retries-retryCount,
                            retryCount);
                    info.finalState = InternalJobState.FAILED;
                }
                break;
            default : // consumer cancelled the job (STOPPED, GIVEN_UP, ERROR)
                if ( this.logger.isDebugEnabled() ) {
                    this.logger.debug("Cancelled job {}", Utility.toString(handler.getJob()));
                }
                info.finalState = InternalJobState.CANCELLED;
                break;
        }

        if ( info.state == Job.JobState.QUEUED && !info.reschedule ) {
            info.state = Job.JobState.GIVEN_UP;
        }
        return info;
    }

    /**
     * Handle job finish and determine whether to reschedule or cancel the job
     */
    private boolean finishedJob(final String jobId,
                                final Job.JobState resultState,
                                final boolean isAsync) {
        this.services.configuration.getAuditLogger().debug("FINISHED {} : {}", resultState, jobId);
        this.logger.debug("Received finish for job {}, resultState={}", jobId, resultState);

        // get job handler
        final JobHandler handler;
        // let's remove the event from our processing list
        synchronized ( this.processingJobsLists ) {
            handler = this.processingJobsLists.remove(jobId);
        }

        if ( !this.running ) {
            this.logger.warn("Queue is not running anymore. Discarding finish for {}", jobId);
            return false;
        }

        if ( handler == null ) {
            this.logger.warn("This job has never been started by this queue: {}", jobId);
            return false;
        }

        // handle the rescheduling of the job
        final RescheduleInfo rescheduleInfo = this.handleReschedule(handler, resultState);

        if ( !rescheduleInfo.reschedule ) {
            // we keep cancelled jobs and succeeded jobs if the queue is configured like this.
            final boolean keepJobs = rescheduleInfo.state != Job.JobState.SUCCEEDED || this.configuration.isKeepJobs();
            handler.finished(rescheduleInfo.state, keepJobs, rescheduleInfo.processingTime);
        } else {
            this.reschedule(handler);
        }
        // update statistics
        this.services.statisticsManager.jobEnded(this.queueName,
                handler.getJob().getTopic(),
                rescheduleInfo.finalState,
                rescheduleInfo.processingTime);
        // send notification
        NotificationUtility.sendNotification(this.services.eventAdmin,
                rescheduleInfo.finalState.getTopic(),
                handler.getJob(), rescheduleInfo.processingTime);

        return rescheduleInfo.reschedule;
    }

    /**
     * @see org.apache.sling.event.jobs.Queue#resume()
     */
    @Override
    public void resume() {
        if ( this.suspendedSince.getAndSet(-1) != -1 ) {
            this.logger.debug("Waking up suspended queue {}", queueName);
            this.startJobs();
        }
    }

    /**
     * @see org.apache.sling.event.jobs.Queue#suspend()
     */
    @Override
    public void suspend() {
        if ( this.suspendedSince.compareAndSet(-1, System.currentTimeMillis()) ) {
            this.logger.debug("Suspending queue {}", queueName);
        }
    }

    /**
     * @see org.apache.sling.event.jobs.Queue#isSuspended()
     */
    @Override
    public boolean isSuspended() {
        return this.suspendedSince.get() != -1;
    }

    /**
     * @see org.apache.sling.event.jobs.Queue#removeAll()
     */
    @Override
    public synchronized void removeAll() {
        final Set<String> topics = this.cache.getTopics();
        logger.debug("Removing all jobs for queue {} : {}", queueName, topics);

        if ( !topics.isEmpty() ) {

            final ResourceResolver resolver = this.services.configuration.createResourceResolver();
            try {
                final Resource baseResource = resolver.getResource(this.services.configuration.getLocalJobsPath());

                // sanity check - should never be null
                if ( baseResource != null ) {
                    final BatchResourceRemover brr = new BatchResourceRemover();

                    for(final String t : topics) {
                        final Resource topicResource = baseResource.getChild(t.replace('/', '.'));
                        if ( topicResource != null ) {
                            JobTopicTraverser.traverse(logger, topicResource, new JobTopicTraverser.JobCallback() {

                                @Override
                                public boolean handle(final JobImpl job) {
                                    final Resource jobResource = topicResource.getResourceResolver().getResource(job.getResourcePath());
                                    // sanity check
                                    if ( jobResource != null ) {
                                        try {
                                            brr.delete(jobResource);
                                        } catch ( final PersistenceException ignore) {
                                            logger.error("Unable to remove job " + job, ignore);
                                            topicResource.getResourceResolver().revert();
                                            topicResource.getResourceResolver().refresh();
                                        }
                                    }
                                    return true;
                                }
                            });
                        }
                    }
                    try {
                        resolver.commit();
                    } catch ( final PersistenceException ignore) {
                        logger.error("Unable to remove jobs", ignore);
                    }
                }
            } finally {
                resolver.close();
            }
        }
    }

    /**
     * @see org.apache.sling.event.jobs.Queue#getState(java.lang.String)
     */
    @Override
    public Object getState(final String key) {
        if ( this.configuration.getType() == Type.ORDERED ) {
            if ( "isSleepingUntil".equals(key) ) {
                return this.isSleepingUntil;
            }
        }
        return null;
    }

    /**
     * @see org.apache.sling.event.jobs.Queue#getStateInfo()
     */
    @Override
    public String getStateInfo() {
        return "outdated=" + this.isOutdated.get() +
                ", suspendedSince=" + this.suspendedSince.get() +
                ", asyncJobs=" + this.asyncCounter.get() +
                ", waitCount=" + this.waitCounter.get() +
                ", jobCount=" + String.valueOf(this.configuration.getMaxParallel() - this.available.availablePermits() +
                (this.configuration.getType() == Type.ORDERED ? ", isSleepingUntil=" + this.isSleepingUntil : ""));
    }

    /**
     * Get the retry delay for a job.
     * @param handler The job handler.
     * @return The retry delay
     */
    private long getRetryDelay(final JobHandler handler) {
        long delay = this.configuration.getRetryDelayInMs();
        if ( handler.getJob().getProperty(JobImpl.PROPERTY_DELAY_OVERRIDE) != null ) {
            delay = handler.getJob().getProperty(JobImpl.PROPERTY_DELAY_OVERRIDE, Long.class);
        } else  if ( handler.getJob().getProperty(Job.PROPERTY_JOB_RETRY_DELAY) != null ) {
            delay = handler.getJob().getProperty(Job.PROPERTY_JOB_RETRY_DELAY, Long.class);
        }
        return delay;
    }

    public boolean stopJob(final JobImpl job) {
        final JobHandler handler;
        synchronized ( this.processingJobsLists ) {
            handler = this.processingJobsLists.get(job.getId());
        }
        if ( handler != null ) {
            handler.stop();
        }
        return handler != null;
    }

    /**
     * Stop all currently running jobs in this queue
     */
    public void stopAllJobs() {
        logger.debug("Stopping all running jobs in queue {}", queueName);
        synchronized ( this.processingJobsLists ) {
            for (final JobHandler handler : this.processingJobsLists.values()) {
                handler.stop();
            }
        }
        logger.debug("All running jobs stopped in queue {}", queueName);
    }

    private void reschedule(final JobHandler handler) {
        // we delay putting back the job until the retry delay is over
        final long delay = this.getRetryDelay(handler);
        if ( delay > 0 ) {
            if ( this.configuration.getType() == Type.ORDERED ) {
                this.cache.setIsBlocked(true);
            }
            handler.addToRetryList();
            final Date fireDate = new Date();
            fireDate.setTime(System.currentTimeMillis() + delay);
            if ( this.configuration.getType() == Type.ORDERED ) {
                this.isSleepingUntil = fireDate.getTime();
            }

            final Runnable t = new Runnable() {
                @Override
                public void run() {
                    try {
                        if ( handler.removeFromRetryList() ) {
                            requeue(handler);
                        }
                        waitCounter.decrementAndGet();
                    } finally {
                        if ( configuration.getType() == Type.ORDERED ) {
                            isSleepingUntil = -1;
                            cache.setIsBlocked(false);
                            startJobs();
                        }
                    }
                }
            };
            this.waitCounter.incrementAndGet();
            final Timer timer = new Timer();
            timer.schedule(new TimerTask() {

                @Override
                public void run() {
                    t.run();
                }
            }, delay);
        } else {
            // put directly into queue
            this.requeue(handler);
        }
    }

    QueueJobCache getCache() {
        return cache;
    }

    OutdatedJobQueueInfo getOutdatedJobQueueInfo() {
        return new OutdatedJobQueueInfo(available, maxParallel, drainage);
    }

    Map<String, JobHandler> getProcessingJobsLists() {
        return processingJobsLists;
    }

    boolean isRunning() {
        return running;
    }
}

