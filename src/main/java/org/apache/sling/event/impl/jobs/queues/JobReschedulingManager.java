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

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A centralized service to manage the delay after which tasks are executed.
 *
 * This service is used to reschedule failed jobs, which are submitted to the queue again
 * after the delay has passed. For that reason the threadpool to manage that delay is quite small,
 * as the re-submission of the jobs into their respective queues is fast.
 */
@Component(service = JobReschedulingManager.class)
@Designate(ocd = JobReschedulingManager.Config.class)
public class JobReschedulingManager {

    @ObjectClassDefinition(name = "Apache Sling Job Rescheduling Manager")
    public @interface Config {

        @AttributeDefinition(name = "thread pool size", description = "number of threads to execute the rescheduling")
        public int threadCount() default 1;
    }

    private static final Logger logger = LoggerFactory.getLogger(JobReschedulingManager.class);

    private ScheduledThreadPoolExecutor executor;

    @Activate
    public void activate(Config config) {
        executor = new ScheduledThreadPoolExecutor(config.threadCount(), new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = new Thread(r, "sling-job-rescheduler-" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                t.setUncaughtExceptionHandler((Thread thread, Throwable e) -> {
                    logger.error("Thread '" + thread.getName() + "' terminated unexpectedly", e);
                });
                return t;
            }
        });
    }

    @Deactivate
    public void deactivate() {
        // submitted tasks which are not yet executed will be dropped (which is fine here)
        executor.shutdown();
    }

    /**
     * Start the provided task with a delay in a fire and forgot way
     * @param task the task to execute
     * @param delayInMilis the delay in milliseconds
     */
    public void reschedule(Runnable task, long delayInMilis) {
        executor.schedule(task, delayInMilis, TimeUnit.MILLISECONDS);
    }
}
