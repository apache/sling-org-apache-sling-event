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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.sling.event.impl.jobs.config.ConfigurationConstants;
import org.apache.sling.event.jobs.QueueConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.junit.PaxExam;

@RunWith(PaxExam.class)
public class RoundRobinMaxParallelTest extends AbstractMaxParallelTest {

    private static final int MAX_PARALLEL = 3;

    private static final int DURATION = 30;

    @Override
    @Before
    public void setup() throws IOException {
        super.setup();

        // create ordered test queue
        final org.osgi.service.cm.Configuration orderedConfig = this.configAdmin.createFactoryConfiguration("org.apache.sling.event.jobs.QueueConfiguration", null);
        final Dictionary<String, Object> orderedProps = new Hashtable<>();
        orderedProps.put(ConfigurationConstants.PROP_NAME, "round-robin-max-parallel");
        orderedProps.put(ConfigurationConstants.PROP_TYPE, QueueConfiguration.Type.TOPIC_ROUND_ROBIN.name());
        orderedProps.put(ConfigurationConstants.PROP_TOPICS, TOPIC_NAME);
        orderedProps.put(ConfigurationConstants.PROP_RETRIES, 2);
        orderedProps.put(ConfigurationConstants.PROP_RETRY_DELAY, 2000L);
        orderedProps.put(ConfigurationConstants.PROP_MAX_PARALLEL, MAX_PARALLEL);
        orderedConfig.update(orderedProps);

        this.sleep(1000L);
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
