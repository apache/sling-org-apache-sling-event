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
package org.apache.sling.event.it;

import org.apache.sling.event.impl.jobs.config.ConfigurationConstants;
import org.apache.sling.event.jobs.QueueConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.factoryConfiguration;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class RoundRobinMaxParallelIT extends AbstractMaxParallelIT {

    private static final int MAX_PARALLEL = 3;

    private static final int DURATION = 50;

    @Configuration
    public Option[] configuration() {
        return options(
            baseConfiguration(),
            factoryConfiguration("org.apache.sling.event.jobs.QueueConfiguration")
                .put(ConfigurationConstants.PROP_NAME, "round-robin-max-parallel")
                .put(ConfigurationConstants.PROP_TYPE, QueueConfiguration.Type.TOPIC_ROUND_ROBIN.name())
                .put(ConfigurationConstants.PROP_TOPICS, TOPIC_NAME)
                .put(ConfigurationConstants.PROP_RETRIES, 2)
                .put(ConfigurationConstants.PROP_RETRY_DELAY, 2000L)
                .put(ConfigurationConstants.PROP_MAX_PARALLEL, MAX_PARALLEL)
                .asOption()
        );
    }

    @Test(timeout=DURATION * 16000L)
    public void testRoundRobinMaxParallel_slow() throws Exception {
        doTestMaxParallel(20, 1717, DURATION);

        assertTrue(max <= MAX_PARALLEL);
    }

    @Test(timeout=DURATION * 16000L)
    public void testRoundRobinMaxParallel_fast() throws Exception {
        doTestMaxParallel(200, 123, DURATION);

        assertTrue(max <= MAX_PARALLEL);
    }
}
