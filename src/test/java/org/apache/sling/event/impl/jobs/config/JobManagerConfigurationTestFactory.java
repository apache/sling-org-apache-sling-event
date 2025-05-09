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

import java.lang.annotation.Annotation;
import java.util.HashMap;

import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration.Config;

public class JobManagerConfigurationTestFactory {

    public static JobManagerConfiguration create(String jobsRoot, 
            ResourceResolverFactory resourceResolverFactory, 
            QueueConfigurationManager queueConfigurationManager) throws NoSuchFieldException {
        final JobManagerConfiguration real = JobManagerConfiguration.newForTest(
                resourceResolverFactory, queueConfigurationManager,
                new HashMap<String, Object>(), new Config() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return null;
            }

            @Override
            public boolean job_consumermanager_disableDistribution() {
                return false;
            }

            @Override
            public long startup_delay() {
                return 0;
            }

            @Override
            public int cleanup_period() {
                return 0;
            }

            public int progresslog_maxCount() {
                return Integer.MAX_VALUE;
            }

            @Override
            public boolean job_processing_condition() {
                return false;
            }
        });
        return real;
    }
    
}
