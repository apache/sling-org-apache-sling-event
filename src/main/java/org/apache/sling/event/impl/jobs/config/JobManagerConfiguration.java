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

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyEvent.Type;
import org.apache.sling.discovery.TopologyEventListener;
import org.apache.sling.event.impl.EnvironmentComponent;
import org.apache.sling.event.impl.discovery.InitDelayingTopologyEventListener;
import org.apache.sling.event.impl.jobs.tasks.CheckTopologyTask;
import org.apache.sling.event.impl.jobs.tasks.FindUnfinishedJobsTask;
import org.apache.sling.event.impl.jobs.tasks.UpgradeTask;
import org.apache.sling.event.impl.support.Environment;
import org.apache.sling.event.impl.support.ResourceHelper;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.serviceusermapping.ServiceUserMapped;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.osgi.service.condition.Condition;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Configuration of the job handling
 *
 */
@Component(immediate=true,
           service=JobManagerConfiguration.class,
           name="org.apache.sling.event.impl.jobs.jcr.PersistenceHandler",
           property = {
                   Constants.SERVICE_VENDOR + "=The Apache Software Foundation",
                   JobManagerConfiguration.PROPERTY_REPOSITORY_PATH + "=" + JobManagerConfiguration.DEFAULT_REPOSITORY_PATH,
                   JobManagerConfiguration.PROPERTY_SCHEDULED_JOBS_PATH + "=" + JobManagerConfiguration.DEFAULT_SCHEDULED_JOBS_PATH,
                   JobManagerConfiguration.PROPERTY_BACKGROUND_LOAD_DELAY + ":Long=" + JobManagerConfiguration.DEFAULT_BACKGROUND_LOAD_DELAY
})
@Designate(ocd = JobManagerConfiguration.Config.class)
public class JobManagerConfiguration {

    @ObjectClassDefinition(name = "Apache Sling Job Manager",
            description="This is the central service of the job handling.")
    public @interface Config {

        @AttributeDefinition(name = "Disable Distribution",
        description="If the distribution is disabled, all jobs will be processed on the leader only! "
                + "Please use this switch with care.")
        boolean job_consumermanager_disableDistribution() default false;

        @AttributeDefinition(name = "Startup Delay",
              description="Specify amount in seconds that job manager waits on startup before starting with job handling. "
                        + "This can be used to allow enough time to restart a cluster before jobs are eventually reassigned.")
        long startup_delay() default 30;

        @AttributeDefinition(name = "Clean-up removed jobs period",
            description = "Specify the periodic interval in minutes (default is 48h - use 0 to disable) after which " +
                    "removed jobs (ERROR or DROPPED) should be cleaned from the repository.")
        int cleanup_period() default 2880;

        @AttributeDefinition(name = "Progress Log message's max count",
                description = "Max number of log messages that can stored by consumer to add information about current state of Job.\n" +
                        "Any attempt to add more information would result into purging of the least recent messages." +
                        "Use 0 to discard all the logs. default is -1 (to indicate infinite). ")
        int progresslog_maxCount() default -1;
    }
    /** Logger. */
    private final Logger logger = LoggerFactory.getLogger("org.apache.sling.event.impl.jobs");

    /** Audit Logger. */
    private final Logger auditLogger = LoggerFactory.getLogger("org.apache.sling.event.jobs.audit");

    /** Default resource path for jobs. */
    public static final String DEFAULT_REPOSITORY_PATH = "/var/eventing/jobs";

    /** Default background load delay. */
    public static final long DEFAULT_BACKGROUND_LOAD_DELAY = 10;

    /** Default resource path for scheduled jobs. */
    public static final String DEFAULT_SCHEDULED_JOBS_PATH = "/var/eventing/scheduled-jobs";

    /** The path where all jobs are stored. */
    public static final String PROPERTY_REPOSITORY_PATH = "repository.path";

    /** The background loader waits this time of seconds after startup before loading events from the repository. (in secs) */
    public static final String PROPERTY_BACKGROUND_LOAD_DELAY = "load.delay";

    /** Configuration property for the scheduled jobs path. */
    public static final String PROPERTY_SCHEDULED_JOBS_PATH = "job.scheduled.jobs.path";

    static JobManagerConfiguration newForTest(ResourceResolverFactory resourceResolverFactory,
            QueueConfigurationManager queueConfigurationManager,
            Map<String, Object> activateProps, Config config) {
        final JobManagerConfiguration jobMgrConfig = new JobManagerConfiguration();
        jobMgrConfig.resourceResolverFactory = resourceResolverFactory;
        jobMgrConfig.queueConfigManager = queueConfigurationManager;
        jobMgrConfig.activate(activateProps, config);
        return jobMgrConfig;
    }

    /** The jobs base path with a slash. */
    private String jobsBasePathWithSlash;

    /** The base path for assigned jobs. */
    private String assignedJobsPath;

    /** The base path for unassigned jobs. */
    private String unassignedJobsPath;

    /** The base path for assigned jobs to the current instance. */
    private String localJobsPath;

    /** The base path for assigned jobs to the current instance - ending with a slash. */
    private String localJobsPathWithSlash;

    private String previousVersionAnonPath;

    private String previousVersionIdentifiedPath;

    private volatile long backgroundLoadDelay;

    private volatile long startupDelay;

    /**
     * The max count of job progress log messages
     */
    private int progressLogMaxCount;

    private volatile InitDelayingTopologyEventListener startupDelayListener;

    private volatile boolean disabledDistribution;

    private String storedCancelledJobsPath;

    private String storedSuccessfulJobsPath;

    /** The resource path where scheduled jobs are stored. */
    private String scheduledJobsPath;

    /** The resource path where scheduled jobs are stored - ending with a slash. */
    private String scheduledJobsPathWithSlash;

    private volatile int historyCleanUpRemovedJobs;

    /** List of topology awares. */
    private final List<ConfigurationChangeListener> listeners = new ArrayList<>();

    /** The environment component. */
    @Reference
    private EnvironmentComponent environment;

    @Reference(policyOption=ReferencePolicyOption.GREEDY)
    private ResourceResolverFactory resourceResolverFactory;

    @Reference
    private QueueConfigurationManager queueConfigManager;

    @Reference(policyOption=ReferencePolicyOption.GREEDY)
    private ServiceUserMapped serviceUserMapped;

    /** Is this still active? */
    private final AtomicBoolean active = new AtomicBoolean(false);

    /** The topology capabilities. */
    private volatile TopologyCapabilities topologyCapabilities;

    /** The condition that determines if job processing is enabled. */
    private volatile Condition jobProcessingEnabledCondition;

    /**
     * Handle binding of the job processing condition.
     * @param condition The condition being bound
     */
    @Reference(
        target = "(osgi.condition.id=true)",
        cardinality = ReferenceCardinality.OPTIONAL,
        policy = ReferencePolicy.DYNAMIC,
        policyOption = ReferencePolicyOption.GREEDY
    )
    protected void bindJobProcessingEnabledCondition(final Condition condition) {
        if (this.jobProcessingEnabledCondition != null) {
            logger.warn("Job processing readiness condition already set - ignoring new condition");
            return;
        }
        this.jobProcessingEnabledCondition = condition;
        // If condition becomes true, trigger maintenance to start processing jobs
        if (condition != null) {
            logger.info("Job processing readiness condition has been set - jobs will be processed");
            notifyListeners();
        }
    }

    /**
     * Handle unbinding of the job processing condition.
     * @param condition The condition being unbound
     */
    protected void unbindJobProcessingEnabledCondition(final Condition condition) {
        if (this.jobProcessingEnabledCondition == condition) {
            this.jobProcessingEnabledCondition = null;
            logger.info("Job processing readiness condition has been removed - jobs will not be processed");
            // Signal jobs to stop before notifying listeners
            stopProcessing();
            notifyListeners();
        }
    }

    /**
     * Check if job processing is enabled.
     * This only affects whether jobs are processed/executed - jobs can still be
     * assigned, stored, and managed through the API even when processing is disabled.
     * @return true if job processing is enabled, false otherwise
     */
    public boolean isJobProcessingEnabled() {
        return jobProcessingEnabledCondition != null;
    }

    /**
     * Activate this component.
     * @param props Configuration properties
     * @param config Configuration properties
     * @throws RuntimeException If the default paths can't be created
     */
    @Activate
    protected void activate(final Map<String, Object> props, final Config config) {
        this.update(props, config);
        this.jobsBasePathWithSlash = PropertiesUtil.toString(props.get(PROPERTY_REPOSITORY_PATH),
                DEFAULT_REPOSITORY_PATH) + '/';

        // create initial resources
        this.assignedJobsPath = this.jobsBasePathWithSlash + "assigned";
        this.unassignedJobsPath = this.jobsBasePathWithSlash + "unassigned";

        this.localJobsPath = this.assignedJobsPath.concat("/").concat(Environment.APPLICATION_ID);
        this.localJobsPathWithSlash = this.localJobsPath.concat("/");

        this.previousVersionAnonPath = this.jobsBasePathWithSlash + "anon";
        this.previousVersionIdentifiedPath = this.jobsBasePathWithSlash + "identified";

        this.storedCancelledJobsPath = this.jobsBasePathWithSlash + "cancelled";
        this.storedSuccessfulJobsPath = this.jobsBasePathWithSlash + "finished";

        this.scheduledJobsPath = PropertiesUtil.toString(props.get(PROPERTY_SCHEDULED_JOBS_PATH),
            DEFAULT_SCHEDULED_JOBS_PATH);
        this.scheduledJobsPathWithSlash = this.scheduledJobsPath + "/";

        this.historyCleanUpRemovedJobs = config.cleanup_period();

        // create initial resources
        final ResourceResolver resolver = this.createResourceResolver();
        try {
            ResourceHelper.getOrCreateBasePath(resolver, this.getLocalJobsPath());
            ResourceHelper.getOrCreateBasePath(resolver, this.getUnassignedJobsPath());
        } catch ( final PersistenceException pe ) {
            logger.error("Unable to create default paths: " + pe.getMessage(), pe);
            throw new RuntimeException(pe);
        } finally {
            resolver.close();
        }
        this.active.set(true);

        // SLING-5560 : use an InitDelayingTopologyEventListener
        if (this.startupDelay > 0) {
            logger.debug("activate: job manager will start in {} sec. ({})", this.startupDelay, config.startup_delay());
            this.startupDelayListener = new InitDelayingTopologyEventListener(startupDelay, new TopologyEventListener() {

                @Override
                public void handleTopologyEvent(TopologyEvent event) {
                    doHandleTopologyEvent(event);
                }
            });
        } else {
            logger.debug("activate: job manager will start without delay. ({}:{})", config.startup_delay(), this.startupDelay);
        }
    }

    /**
     * Update with a new configuration
     */
    @Modified
    protected void update(final Map<String, Object> props, final Config config) {
        this.disabledDistribution = config.job_consumermanager_disableDistribution();
        this.backgroundLoadDelay = PropertiesUtil.toLong(props.get(PROPERTY_BACKGROUND_LOAD_DELAY), DEFAULT_BACKGROUND_LOAD_DELAY);
        // SLING-5560: note that currently you can't change the startupDelay to have
        // an immediate effect - it will only have an effect on next activation.
        // (as 'startup delay runnable' is already scheduled in activate)
        this.startupDelay = config.startup_delay();

        if (config.progresslog_maxCount() < 0) {
            this.progressLogMaxCount = Integer.MAX_VALUE;
        } else {
            this.progressLogMaxCount = config.progresslog_maxCount();
        }

    }

    /**
     * Deactivate
     */
    @Deactivate
    protected void deactivate() {
        this.active.set(false);
        if ( this.startupDelayListener != null) {
            this.startupDelayListener.dispose();
            this.startupDelayListener = null;
        }
        this.stopProcessing();
    }

    public int getHistoryCleanUpRemovedJobs() {
        return this.historyCleanUpRemovedJobs;
    }
    /**
     * Is this component still active?
     * @return Active?
     */
    public boolean isActive() {
        return this.active.get();
    }

    /**
     * Create a new resource resolver for reading and writing the resource tree.
     * The resolver needs to be closed by the client.
     * This ResourceResolver provides read and write access to all resources relevant for the event
     * and job handling.
     * 
     * @return A resource resolver or {@code null} if the component is already deactivated.
     * @throws RuntimeException if the resolver can't be created.
     */
    public ResourceResolver createResourceResolver() {
        ResourceResolver resolver = null;
        final ResourceResolverFactory factory = this.resourceResolverFactory;
        if ( factory != null ) {
            try {
                resolver = this.resourceResolverFactory.getServiceResourceResolver(null);
            } catch ( final LoginException le) {
                logger.error("Unable to create new resource resolver: " + le.getMessage(), le);
                throw new RuntimeException(le);
            }
        }
        return resolver;
    }

    /**
     * Get the current topology capabilities.
     * @return The capabilities or {@code null}
     */
    public TopologyCapabilities getTopologyCapabilities() {
        return this.topologyCapabilities;
    }

    public QueueConfigurationManager getQueueConfigurationManager() {
        return this.queueConfigManager;
    }

    /**
     * Get main logger.
     * @return The main logger.
     */
    public Logger getMainLogger() {
        return this.logger;
    }

    /**
     * Get the resource path for all assigned jobs.
     * @return The path - does not end with a slash.
     */
    public String getAssginedJobsPath() {
        return this.assignedJobsPath;
    }

    /**
     * Get the resource path for all unassigned jobs.
     * @return The path - does not end with a slash.
     */
    public String getUnassignedJobsPath() {
        return this.unassignedJobsPath;
    }

    /**
     * Get the resource path for all jobs assigned to the current instance
     * @return The path - does not end with a slash
     */
    public String getLocalJobsPath() {
        return this.localJobsPath;
    }

    /** Counter for jobs without an id. */
    private final AtomicLong jobCounter = new AtomicLong(0);

    /**
     * Create a unique job path (folder and name) for the job.
     */
    public String getUniquePath(final String targetId,
            final String topic,
            final String jobId,
            final Map<String, Object> jobProperties) {
        final String topicName = topic.replace('/', '.');
        final StringBuilder sb = new StringBuilder();
        if ( targetId != null ) {
            sb.append(this.getAssginedJobsPath());
            sb.append('/');
            sb.append(targetId);
        } else {
            sb.append(this.getUnassignedJobsPath());
        }
        sb.append('/');
        sb.append(topicName);
        sb.append('/');
        sb.append(jobId);

        return sb.toString();
    }

    /**
     * Get the unique job id
     */
    public String getUniqueId(final String jobTopic) {
        final Calendar now = Calendar.getInstance();
        final StringBuilder sb = new StringBuilder();
        sb.append(now.get(Calendar.YEAR));
        sb.append('/');
        sb.append(now.get(Calendar.MONTH) + 1);
        sb.append('/');
        sb.append(now.get(Calendar.DAY_OF_MONTH));
        sb.append('/');
        sb.append(now.get(Calendar.HOUR_OF_DAY));
        sb.append('/');
        sb.append(now.get(Calendar.MINUTE));
        sb.append('/');
        sb.append(Environment.APPLICATION_ID);
        sb.append('_');
        sb.append(jobCounter.getAndIncrement());

        return sb.toString();
    }

    public boolean isLocalJob(final String jobPath) {
        return jobPath != null && jobPath.startsWith(this.localJobsPathWithSlash);
    }

    public boolean isJob(final String jobPath) {
        return jobPath.startsWith(this.jobsBasePathWithSlash);
    }

    public String getJobsBasePathWithSlash() {
        return this.jobsBasePathWithSlash;
    }

    public int getProgressLogMaxCount() {
        return this.progressLogMaxCount;
    }

    public String getPreviousVersionAnonPath() {
        return this.previousVersionAnonPath;
    }

    public String getPreviousVersionIdentifiedPath() {
        return this.previousVersionIdentifiedPath;
    }

    public boolean disableDistribution() {
        return this.disabledDistribution;
    }

    public String getStoredCancelledJobsPath() {
        return this.storedCancelledJobsPath;
    }

    public String getStoredSuccessfulJobsPath() {
        return this.storedSuccessfulJobsPath;
    }

    /**
     * Get the storage path for finished jobs.
     * @param topic Topic of the finished job
     * @param jobId The job id of the finished job.
     * @param isSuccess Whether processing was successful or not
     * @return The complete storage path
     */
    public String getStoragePath(final String topic, final String jobId, final boolean isSuccess) {
        final String topicName = topic.replace('/', '.');
        final StringBuilder sb = new StringBuilder();
        if ( isSuccess ) {
            sb.append(this.getStoredSuccessfulJobsPath());
        } else {
            sb.append(this.getStoredCancelledJobsPath());
        }
        sb.append('/');
        sb.append(topicName);
        sb.append('/');
        sb.append(jobId);

        return sb.toString();

    }

    /**
     * Check whether this is a storage path.
     */
    public boolean isStoragePath(final String path) {
        return path.startsWith(this.storedCancelledJobsPath) || path.startsWith(this.storedSuccessfulJobsPath);
    }

    /**
     * Get the scheduled jobs path
     * @param slash If {@code false} the path is returned, if {@code true} the path appended with a slash is returned.
     * @return The path for the scheduled jobs
     */
    public String getScheduledJobsPath(final boolean slash) {
        return (slash ? this.scheduledJobsPathWithSlash : this.scheduledJobsPath);
    }

    /**
     * Stop processing
     */
    private void stopProcessing() {
        logger.debug("Stopping job processing...");
        final TopologyCapabilities caps = this.topologyCapabilities;

        if ( caps != null ) {
            // deactivate old capabilities - this stops all background processes
            caps.deactivate();
            this.topologyCapabilities = null;

            // stop all listeners
            this.notifyListeners();
        }
        logger.debug("Job processing stopped");
    }

    /**
     * Start processing
     * @param eventType The event type
     * @param newCaps The new capabilities
     */
    private void startProcessing(final Type eventType, final TopologyCapabilities newCaps) {
        logger.debug("Starting job processing...");
        // create new capabilities and update view
        this.topologyCapabilities = newCaps;

        // before we propagate the new topology we do some maintenance
        if ( eventType == Type.TOPOLOGY_INIT ) {
            final UpgradeTask task = new UpgradeTask(this);
            task.run();

            final FindUnfinishedJobsTask rt = new FindUnfinishedJobsTask(this);
            rt.run();

            final CheckTopologyTask mt = new CheckTopologyTask(this);
            mt.fullRun();

            notifyListeners();
        } else {
            // and run checker again in some seconds (if leader)
            // notify listeners afterwards
            final Timer timer = new Timer();
            timer.schedule(new TimerTask()
            {

                @Override
                public void run() {
                    if ( newCaps == topologyCapabilities && newCaps.isActive()) {
                        // start listeners
                        notifyListeners();
                        if ( newCaps.isLeader() && newCaps.isActive() ) {
                            final CheckTopologyTask mt = new CheckTopologyTask(JobManagerConfiguration.this);
                            mt.fullRun();
                        }
                    }
                }
            }, this.backgroundLoadDelay * 1000);
        }
        logger.debug("Job processing started");
    }

    /**
     * Notify all listeners
     */
    private void notifyListeners() {
        synchronized ( this.listeners ) {
            final TopologyCapabilities caps = this.topologyCapabilities;
            for(final ConfigurationChangeListener l : this.listeners) {
                l.configurationChanged(caps != null);
            }
        }
    }

    /**
     * This method is invoked asynchronously from the TopologyHandler.
     * Therefore this method can't be invoked concurrently
     * @see org.apache.sling.discovery.TopologyEventListener#handleTopologyEvent(org.apache.sling.discovery.TopologyEvent)
     */
    public void handleTopologyEvent(TopologyEvent event) {
        if ( this.startupDelayListener != null ) {
            // with startup.delay > 0
            this.startupDelayListener.handleTopologyEvent(event);
        } else {
            // classic (startup.delay <= 0)
            this.logger.debug("Received topology event {}", event);
            doHandleTopologyEvent(event);
        }
    }

    void doHandleTopologyEvent(final TopologyEvent event) {

        // check if there is a change of properties which doesn't affect us
        // but we need to use the new view !
        boolean stopProcessing = true;
        if ( event.getType() == Type.PROPERTIES_CHANGED ) {
            final Map<String, String> newAllInstances = TopologyCapabilities.getAllInstancesMap(event.getNewView());
            if ( this.topologyCapabilities != null && this.topologyCapabilities.isSame(newAllInstances) ) {
                logger.debug("No changes in capabilities - updating topology capabilities with new view");
                stopProcessing = false;
            }
        }

        final TopologyEvent.Type eventType = event.getType();

        if ( eventType == Type.TOPOLOGY_CHANGING ) {
           this.stopProcessing();

        } else if ( eventType == Type.TOPOLOGY_INIT
            || event.getType() == Type.TOPOLOGY_CHANGED
            || event.getType() == Type.PROPERTIES_CHANGED ) {

            if ( stopProcessing ) {
                this.stopProcessing();
            }

            this.startProcessing(eventType, new TopologyCapabilities(event.getNewView(), this));
        }
    }

    /**
     * Add a topology aware listener
     * @param service Listener to notify about changes.
     */
    public void addListener(final ConfigurationChangeListener service) {
        synchronized ( this.listeners ) {
            this.listeners.add(service);
            service.configurationChanged(this.topologyCapabilities != null);
        }
    }

    /**
     * Remove a topology aware listener
     * @param service Listener to notify about changes.
     */
    public void removeListener(final ConfigurationChangeListener service) {
        synchronized ( this.listeners )  {
            this.listeners.remove(service);
        }
    }

    private final Map<String, Job> retryList = new HashMap<>();

    public void addJobToRetryList(final Job job) {
        synchronized ( retryList ) {
            retryList.put(job.getId(), job);
        }
    }

    public List<Job> clearJobRetryList() {
        final List<Job> result = new ArrayList<>();
        synchronized ( this.retryList ) {
            result.addAll(retryList.values());
            retryList.clear();
        }
        return result;
    }

    public boolean removeJobFromRetryList(final Job job) {
        synchronized ( retryList ) {
            return retryList.remove(job.getId()) != null;
        }
    }

    public Job getJobFromRetryList(final String jobId) {
        synchronized ( retryList ) {
            return retryList.get(jobId);
        }
    }

    /**
     * The audit logger is logging actions for auditing.
     * @return The logger
     */
    public Logger getAuditLogger() {
        return this.auditLogger;
    }
}
