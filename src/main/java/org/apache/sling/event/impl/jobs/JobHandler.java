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
package org.apache.sling.event.impl.jobs;


import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager.QueueInfo;
import org.apache.sling.event.impl.jobs.config.TopologyCapabilities;
import org.apache.sling.event.impl.support.ResourceHelper;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.Queue;
import org.apache.sling.event.jobs.consumer.JobExecutor;


/**
 * This object adds actions to a {@link JobImpl}.
 */
public class JobHandler {

    private final JobImpl job;

    public volatile long started = -1;

    private volatile boolean isStopped = false;

    private final JobManagerConfiguration configuration;

    /**
     * Max number of log message that can stored by consumer to add information about current state of Job.
     */
    private final int progressLogMaxCount;

    private final JobExecutor consumer;

    public JobHandler(final JobImpl job,
            final JobExecutor consumer,
            final JobManagerConfiguration configuration) {
        this.job = job;
        this.consumer = consumer;
        this.configuration = configuration;
        this.progressLogMaxCount = configuration.getProgressLogMaxCount();
    }

    public JobImpl getJob() {
        return this.job;
    }

    public int getProgressLogMaxCount() {
        return progressLogMaxCount;
    }

    public JobExecutor getConsumer() {
        return this.consumer;
    }

    public boolean startProcessing(final Queue queue) {
        this.isStopped = false;
        return this.persistJobProperties(this.job.prepare(queue));
    }

    /**
     * Reschedule the job
     * Update the retry count and remove the started time.
     * @return <code>true</code> if rescheduling was successful, <code>false</code> otherwise.
     */    
    public boolean reschedule() {
        return withJobResource((jobResource,mvm) -> {
            mvm.put(Job.PROPERTY_JOB_RETRY_COUNT, job.getProperty(Job.PROPERTY_JOB_RETRY_COUNT, Integer.class));
            if ( job.getProperty(Job.PROPERTY_RESULT_MESSAGE) != null ) {
                mvm.put(Job.PROPERTY_RESULT_MESSAGE, job.getProperty(Job.PROPERTY_RESULT_MESSAGE));
            }
            mvm.remove(Job.PROPERTY_JOB_STARTED_TIME);
            mvm.put(JobImpl.PROPERTY_JOB_QUEUED, Calendar.getInstance());
            try {
                jobResource.getResourceResolver().commit();
                return true;
            } catch ( final PersistenceException pe ) {
                this.configuration.getMainLogger().debug("Unable to update reschedule properties for job " + job.getId(), pe);
            }
            return false;
        });
    }

    /**
     * Finish a job.
     * @param state The state of the processing
     * @param keepJobInHistory whether to keep the job in the job history.
     * @param duration the duration of the processing.
     */    
    public void finished(final Job.JobState state, final boolean keepJobInHistory, final Long duration) {
        final boolean isSuccess = (state == Job.JobState.SUCCEEDED);
        withJobResource((jobResource,mvm) -> {
            try {
                ResourceResolver rr = jobResource.getResourceResolver();
                String newPath = null;
                if (keepJobInHistory) {
                    newPath = this.configuration.getStoragePath(job.getTopic(), job.getId(), isSuccess);
                    final Map<String, Object> props = new HashMap<>(mvm);
                    props.put(JobImpl.PROPERTY_FINISHED_STATE, state.name());
                    if (isSuccess) {
                        // we set the finish date to start date + duration
                        final Date finishDate = new Date();
                        finishDate.setTime(job.getProcessingStarted().getTime().getTime() + duration);
                        final Calendar finishCal = Calendar.getInstance();
                        finishCal.setTime(finishDate);
                        props.put(JobImpl.PROPERTY_FINISHED_DATE, finishCal);
                    } else {
                        // current time is good enough
                        props.put(JobImpl.PROPERTY_FINISHED_DATE, Calendar.getInstance());
                    }
                    if (job.getProperty(Job.PROPERTY_RESULT_MESSAGE) != null) {
                        props.put(Job.PROPERTY_RESULT_MESSAGE, job.getProperty(Job.PROPERTY_RESULT_MESSAGE));
                    }
                    ResourceHelper.getOrCreateResource(rr, newPath, props);
                }
                rr.delete(jobResource);
                rr.commit();

                if (keepJobInHistory && configuration.getMainLogger().isDebugEnabled()) {
                    if (isSuccess) {
                        configuration.getMainLogger().debug("Kept successful job {} at {}", Utility.toString(job),
                                newPath);
                    } else {
                        configuration.getMainLogger().debug("Moved cancelled job {} to {}", Utility.toString(job),
                                newPath);
                    }
                }
            } catch (final PersistenceException pe) {
                this.configuration.getMainLogger().warn("Unable to finish job " + job.getId(), pe);
            }
            return false; // this return value is ignored
        });
    }
    
    

    /**
     * Reassign to a new instance.
     */
    public void reassign() {
        final QueueInfo queueInfo = this.configuration.getQueueConfigurationManager().getQueueInfo(job.getTopic());
        // Sanity check if queue configuration has changed
        final TopologyCapabilities caps = this.configuration.getTopologyCapabilities();
        final String targetId = (caps == null ? null
                : caps.detectTarget(job.getTopic(), job.getProperties(), queueInfo));

        withJobResource((jobResource, mvm) -> {
            final String newPath = this.configuration.getUniquePath(targetId, job.getTopic(), job.getId(),
                    job.getProperties());

            final Map<String, Object> props = new HashMap<>(mvm);
            props.remove(Job.PROPERTY_JOB_QUEUE_NAME);
            if (targetId == null) {
                props.remove(Job.PROPERTY_JOB_TARGET_INSTANCE);
            } else {
                props.put(Job.PROPERTY_JOB_TARGET_INSTANCE, targetId);
            }
            props.remove(Job.PROPERTY_JOB_STARTED_TIME);

            try {
                ResourceResolver r = jobResource.getResourceResolver();
                ResourceHelper.getOrCreateResource(r, newPath, props);
                r.delete(jobResource);
                r.commit();
            } catch (final PersistenceException pe) {
                this.configuration.getMainLogger().warn("Unable to reassign job " + job.getId(), pe);
            }
            return true; // this return value is ignored
        });
    }

    /**
     * Update the property of a job in the resource tree
     * @param propNames the property names to update
     * @return {@code true} if the update was successful.
     */    
    public boolean persistJobProperties(final String... propNames) {
        if (propNames == null) {
            return true;
        }
        return withJobResource((jobResource,mvm) -> {
            for(final String propName : propNames) {
                final Object val = job.getProperty(propName);
                if ( val != null ) {
                    if ( val.getClass().isEnum() ) {
                        mvm.put(propName, val.toString());
                    } else {
                        mvm.put(propName, val);
                    }
                } else {
                    mvm.remove(propName);
                }
            }
            try {
                jobResource.getResourceResolver().commit();
                return true;
            } catch (PersistenceException ignore) {
                this.configuration.getMainLogger().debug("Unable to persist properties", ignore);
            }
            return false;
        });
        
    }

    public boolean isStopped() {
        return this.isStopped;
    }

    public void stop() {
        this.isStopped = true;
    }

    public void addToRetryList() {
        this.configuration.addJobToRetryList(this.job);

    }

    public boolean removeFromRetryList() {
        return this.configuration.removeJobFromRetryList(this.job);
    }

    @Override
    public int hashCode() {
        return this.job.getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if ( ! (obj instanceof JobHandler) ) {
            return false;
        }
        return this.job.getId().equals(((JobHandler)obj).job.getId());
    }

    @Override
    public String toString() {
        return "JobHandler(" + this.job.getId() + ")";
    }
    
    /**
     * Helper method to execute a function on the job resource. Performs all necessary checks and validations
     * so that the function does not need to perform any null checks and such.
     * 
     * The second parameter is a ModifiableValueMap (non-null) adapted from the JobResource,
     * so both a read-only and a read/write case can be implemented.
     *
     * @param func the function to execute
     * @return the status (which is passed thru from func
     */
    private boolean withJobResource (BiFunction<Resource,ModifiableValueMap,Boolean> func) {
        try (ResourceResolver resolver = this.configuration.createResourceResolver()) {
            Resource jobResource = resolver.getResource(job.getResourcePath());
            if (jobResource == null) {
                this.configuration.getMainLogger().debug("No job resource found at {}", job.getResourcePath());
                return false;  
            }
            // This implicitly assumes that the ResourceResolver provided by the configuration allows 
            // r/w access to the jobResource.
            ModifiableValueMap mvm = jobResource.adaptTo(ModifiableValueMap.class);
            if (mvm == null){
                this.configuration.getMainLogger().debug("Cannot adapt resource {} to ModifiableValueMap, no write permissions?", job.getResourcePath());
                return false;  
            }
            return func.apply(jobResource,mvm);
        }
        
    }

}