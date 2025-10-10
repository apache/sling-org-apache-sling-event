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

import java.util.Collections;

import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager.QueueInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueueConfigurationManagerTest {

    @Test
    public void testMultipleMatchingConfigsWithSameRankingAndSameTopic() {
        QueueConfigurationManager configMgr = new QueueConfigurationManager();
        configMgr.bindConfig(InternalQueueConfiguration.fromConfiguration(
                Collections.<String, Object>emptyMap(),
                InternalQueueConfigurationTest.createConfig(new String[] {"topic1"}, "queue1", 2, 0)));
        configMgr.bindConfig(InternalQueueConfiguration.fromConfiguration(
                Collections.<String, Object>emptyMap(),
                InternalQueueConfigurationTest.createConfig(new String[] {"topic1"}, "queue2", 2, 0)));
        configMgr.bindConfig(InternalQueueConfiguration.fromConfiguration(
                Collections.<String, Object>emptyMap(),
                InternalQueueConfigurationTest.createConfig(new String[] {"topic1"}, "queue3", 2, -3)));
        QueueInfo queueInfo = configMgr.getQueueInfo("topic1");
        assertEquals("queue1", queueInfo.queueName); // first queue wins
    }

    @Test
    public void testMultipleMatchingConfigsWithSameRankingAndDifferentTopic() {
        QueueConfigurationManager configMgr = new QueueConfigurationManager();
        configMgr.bindConfig(InternalQueueConfiguration.fromConfiguration(
                Collections.<String, Object>emptyMap(),
                InternalQueueConfigurationTest.createConfig(new String[] {"*"}, "queue1", 2, 0)));
        configMgr.bindConfig(InternalQueueConfiguration.fromConfiguration(
                Collections.<String, Object>emptyMap(),
                InternalQueueConfigurationTest.createConfig(new String[] {"topic1/test"}, "queue2", 2, 0)));
        QueueInfo queueInfo = configMgr.getQueueInfo("topic1/test");
        assertEquals("queue1", queueInfo.queueName); // first queue wins (although topic in config is more generic)
    }

    @Test
    public void testMultipleMatchingConfigsWithDifferentRanking() {
        QueueConfigurationManager configMgr = new QueueConfigurationManager();
        configMgr.bindConfig(InternalQueueConfiguration.fromConfiguration(
                Collections.<String, Object>emptyMap(),
                InternalQueueConfigurationTest.createConfig(new String[] {"topic1/test"}, "queue1", 2, 1)));
        configMgr.bindConfig(InternalQueueConfiguration.fromConfiguration(
                Collections.<String, Object>emptyMap(),
                InternalQueueConfigurationTest.createConfig(new String[] {"topic1/*"}, "queue2", 2, 2)));
        QueueInfo queueInfo = configMgr.getQueueInfo("topic1/test");
        assertEquals("queue2", queueInfo.queueName); // highest ranking wins
    }
}
