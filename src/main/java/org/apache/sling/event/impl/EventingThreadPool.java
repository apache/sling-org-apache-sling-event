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
package org.apache.sling.event.impl;

import org.apache.sling.commons.threads.ModifiableThreadPoolConfig;
import org.apache.sling.commons.threads.ThreadPool;
import org.apache.sling.commons.threads.ThreadPoolConfig;
import org.apache.sling.commons.threads.ThreadPoolConfig.ThreadPriority;
import org.apache.sling.commons.threads.ThreadPoolManager;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The configurable eventing thread pool.
 */
@Component(service = EventingThreadPool.class,
           property = {
              Constants.SERVICE_VENDOR + "=The Apache Software Foundation"
})
@Designate(ocd = EventingThreadPool.Config.class)
public class EventingThreadPool implements ThreadPool {

    @ObjectClassDefinition(name = "Apache Sling Job Thread Pool",
        description="This is the thread pool used by the Apache Sling job handling. The "
                  + "threads from this pool are merely used for executing jobs. By limiting this pool, it is "
                  + "possible to limit the maximum number of parallel processed jobs - regardless of the queue "
                  + "configuration.")
    public @interface Config {

        @AttributeDefinition(name = "Pool Size",
                description = "The size of the thread pool. This pool is used to execute jobs and therefore "
                        + "limits the maximum number of jobs executed in parallel. "
                        + "A value of -1 is substituted with the number of available processors. "
                        + "A decimal number between 0.0 and 1.0 is treated as a fraction of available processors. "
                        + "For example 0.5 means half of the available processors. ")
        double minPoolSize() default 35;
    }

    /**
     * Logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(EventingThreadPool.class);

    @Reference(policyOption=ReferencePolicyOption.GREEDY)
    private ThreadPoolManager threadPoolManager;

    /** The real thread pool used. */
    private org.apache.sling.commons.threads.ThreadPool threadPool;

    public EventingThreadPool() {
        // default constructor
    }

    public EventingThreadPool(final ThreadPoolManager tpm, final int poolSize) {
        this.threadPoolManager = tpm;
        this.configure(poolSize);
    }

    public void release() {
        this.deactivate();
    }

    /**
     * Activate this component.
     */
    @Activate
    @Modified
    protected void activate(final Config config) {
        this.configure(config.minPoolSize());
    }

    private void configure(final double maxPoolSize) {
        final int poolSize = ProcessorBasedCalculator.calculate(logger, maxPoolSize);
        final ModifiableThreadPoolConfig config = new ModifiableThreadPoolConfig();
        config.setMinPoolSize(poolSize);
        config.setMaxPoolSize(config.getMinPoolSize());
        config.setQueueSize(-1); // unlimited
        config.setShutdownGraceful(true);
        config.setPriority(ThreadPriority.NORM);
        config.setDaemon(true);
        this.threadPool = threadPoolManager.create(config, "Apache Sling Job Thread Pool");
    }

    /**
     * Deactivate this component.
     */
    @Deactivate
    protected void deactivate() {
        this.threadPoolManager.release(this.threadPool);
    }

    /**
     * @see org.apache.sling.commons.threads.ThreadPool#execute(java.lang.Runnable)
     */
    @Override
    public void execute(final Runnable runnable) {
        threadPool.execute(runnable);
    }

    /**
     * @see org.apache.sling.commons.threads.ThreadPool#getConfiguration()
     */
    @Override
    public ThreadPoolConfig getConfiguration() {
        return threadPool.getConfiguration();
    }

    /**
     * @see org.apache.sling.commons.threads.ThreadPool#getName()
     */
    @Override
    public String getName() {
        return threadPool.getName();
    }
}
