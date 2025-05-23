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
package org.apache.sling.event.impl.jobs.console;


import org.apache.felix.inventory.Format;
import org.apache.sling.event.impl.JsonTestBase;
import org.apache.sling.event.impl.jobs.JobConsumerManager;
import org.apache.sling.event.impl.jobs.config.InternalQueueConfiguration;
import org.apache.sling.event.impl.jobs.config.JobManagerConfiguration;
import org.apache.sling.event.impl.jobs.config.QueueConfigurationManager;
import org.apache.sling.event.jobs.JobManager;
import org.apache.sling.event.jobs.Queue;
import org.apache.sling.event.jobs.QueueConfiguration;
import org.apache.sling.event.jobs.ScheduleInfo;
import org.apache.sling.event.jobs.ScheduledJobInfo;
import org.apache.sling.event.jobs.Statistics;
import org.apache.sling.event.jobs.TopicStatistics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class InventoryPluginTest extends JsonTestBase {
    @Mock
    private JobManager jobManager;

    @Mock
    private JobManagerConfiguration configuration;

    @Mock
    private JobConsumerManager jobConsumerManager;

    @InjectMocks
    private InventoryPlugin inventoryPlugin;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        Mockito.when(jobManager.getStatistics()).thenReturn(Mockito.mock(Statistics.class));
        Queue mockQueue = Mockito.mock(Queue.class);
        Mockito.when(mockQueue.getStatistics()).thenReturn(Mockito.mock(Statistics.class));

        ScheduledJobInfo mockScheduledJobInfo = Mockito.mock(ScheduledJobInfo.class);
        Mockito.when(mockScheduledJobInfo.getJobTopic()).thenReturn("topic");
        ScheduleInfo mockScheduleInfo1 = Mockito.mock(ScheduleInfo.class);
        Mockito.when(mockScheduleInfo1.getType()).thenReturn(ScheduleInfo.ScheduleType.HOURLY);
        Mockito.when(mockScheduleInfo1.getMinuteOfHour()).thenReturn(40);
        ScheduleInfo mockScheduleInfo2 = Mockito.mock(ScheduleInfo.class);
        Mockito.when(mockScheduleInfo2.getType()).thenReturn(ScheduleInfo.ScheduleType.CRON);
        Mockito.when(mockScheduleInfo2.getExpression()).thenReturn("0 * * * *");
        Mockito.when(mockScheduledJobInfo.getSchedules()).thenReturn(new ArrayList<>(Arrays.asList(mockScheduleInfo1,
         mockScheduleInfo2)));
        Mockito.when(jobManager.getScheduledJobs()).thenReturn(new ArrayList<>(Arrays.asList(mockScheduledJobInfo)));

        QueueConfiguration mockConfiguration = Mockito.mock(QueueConfiguration.class);
        Mockito.when(mockConfiguration.getType()).thenReturn(QueueConfiguration.Type.ORDERED);
        Mockito.when(mockConfiguration.getTopics()).thenReturn(new String[]{"topic-1", "topic-2"});
        Mockito.when(mockQueue.getConfiguration()).thenReturn(mockConfiguration);
        Mockito.when(jobManager.getQueues()).thenReturn(new ArrayList<>(Arrays.asList(mockQueue)));

        TopicStatistics topicStatistics = Mockito.mock(TopicStatistics.class);
        Mockito.when(jobManager.getTopicStatistics()).thenReturn(new ArrayList<>(Arrays.asList(topicStatistics)));

        QueueConfigurationManager mockQueueConfigurationManager = Mockito.mock(QueueConfigurationManager.class);
        InternalQueueConfiguration mockMainInternalQueueConfiguration = Mockito.mock(InternalQueueConfiguration.class);
        Mockito.when(mockMainInternalQueueConfiguration.getType()).thenReturn(QueueConfiguration.Type.ORDERED);
        Mockito.when(mockQueueConfigurationManager.getMainQueueConfiguration()).thenReturn(mockMainInternalQueueConfiguration);
        InternalQueueConfiguration mockConfigInternalQueueConfiguration =
                Mockito.mock(InternalQueueConfiguration.class);
        Mockito.when(mockConfigInternalQueueConfiguration.getType()).thenReturn(QueueConfiguration.Type.ORDERED);
        Mockito.when(mockQueueConfigurationManager.getConfigurations()).thenReturn(new InternalQueueConfiguration[]{mockConfigInternalQueueConfiguration});
        Mockito.when(configuration.getQueueConfigurationManager()).thenReturn(mockQueueConfigurationManager);
    }

    @Test
    public void printTextFormat() {
        StringWriter writerOutput = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writerOutput);

        inventoryPlugin.print(printWriter, Format.TEXT, false);

        List<String> outputContains = new ArrayList<String>() {{
            add("Apache Sling Job Handling");
            add("Overall Statistics");
            add("Local topic consumers:");
            add("Queued Jobs : 0");
            add("Active Jobs : 0");
            add("Jobs : 0");
            add("Finished Jobs : 0");
            add("Failed Jobs : 0");
            add("Cancelled Jobs : 0");
            add("Processed Jobs : 0");
            add("Topology Capabilities");
            add("No topology information available");
            add("Scheduled Jobs");
            add("Job Topic< : topic");
            add("Schedules : HOURLY 40, CRON 0 * * * *");
            add("Apache Sling Job Handling - Job Queue Configurations");
            add("Valid : false");
            add("Type : Ordered");
            add("Max Parallel : 0");
            add("Max Retries : 0");
            add("Retry Delay : 0 ms");
            add("Ranking : 0");
        }};

        String text = writerOutput.toString();
        outputContains.forEach((e) -> {
            assertThat(text, containsString(e));
        });
    }

    @Test
    public void printJsonFormat() {
        try {
            final String json = queryInventoryJSON(inventoryPlugin);

            Map<String, Object> expectedJsonPaths = new HashMap<String, Object>(){{
                put("$.statistics", null);
                put("$.statistics.startTime", 0);
                put("$.statistics.lastActivatedJobTime", 0);
                put("$.statistics.lastFinishedJobTime", 0);
                put("$.statistics.numberOfQueuedJobs", 0);
                put("$.statistics.numberOfActiveJobs", 0);
                put("$.statistics.numberOfJobs", 0);
                put("$.statistics.numberOfFinishedJobs", 0);
                put("$.statistics.numberOfFailedJobs", 0);
                put("$.statistics.numberOfCancelledJobs", 0);
                put("$.statistics.numberOfProcessedJobs", 0);
                put("$.statistics.averageProcessingTime", 0);
                put("$.statistics.averageWaitingTime", 0);
                put("$.scheduledJobs", null);
                put("$.scheduledJobs[0].jobTopic", "topic");
                put("$.scheduledJobs[0].schedules", null);
                put("$.scheduledJobs[0].schedules[0].type", "HOURLY");
                put("$.scheduledJobs[0].schedules[0].schedule", "40");
                put("$.scheduledJobs[0].schedules[1].type", "CRON");
                put("$.scheduledJobs[0].schedules[1].schedule", "0 * * * *");
                put("$.queues", null);
                put("$.queues[0].suspended", false);
                put("$.queues[0].statistics", null);
                put("$.queues[0].statistics.startTime", 0);
                put("$.queues[0].statistics.lastActivatedJobTime", 0);
                put("$.queues[0].statistics.lastActivatedJobTime", 0);
                put("$.queues[0].statistics.lastFinishedJobTime", 0);
                put("$.queues[0].statistics.numberOfQueuedJobs", 0);
                put("$.queues[0].statistics.numberOfActiveJobs", 0);
                put("$.queues[0].statistics.numberOfJobs", 0);
                put("$.queues[0].statistics.numberOfFinishedJobs", 0);
                put("$.queues[0].statistics.numberOfFailedJobs", 0);
                put("$.queues[0].statistics.numberOfCancelledJobs", 0);
                put("$.queues[0].statistics.numberOfProcessedJobs", 0);
                put("$.queues[0].statistics.averageProcessingTime", 0);
                put("$.queues[0].statistics.averageWaitingTime", 0);
                put("$.queues[0].configuration", null);
                put("$.queues[0].configuration.type", "ORDERED");
                put("$.queues[0].configuration.topics", new String[]{"topic-1", "topic-2"});
                put("$.queues[0].configuration.maxParallel", 0);
                put("$.queues[0].configuration.maxRetries", 0);
                put("$.queues[0].configuration.retryDelayInMs", 0);
                put("$.topicStatistics", null);
                put("$.topicStatistics[0].lastActivatedJobTime", 0);
                put("$.topicStatistics[0].lastFinishedJobTime", 0);
                put("$.topicStatistics[0].numberOfFinishedJobs", 0);
                put("$.topicStatistics[0].numberOfFailedJobs", 0);
                put("$.topicStatistics[0].numberOfCancelledJobs", 0);
                put("$.topicStatistics[0].numberOfProcessedJobs", 0);
                put("$.topicStatistics[0].averageProcessingTime", 0);
                put("$.topicStatistics[0].averageWaitingTime", 0);
                put("$.configurations", null);
                put("$.configurations[0].valid", false);
                put("$.configurations[0].type", "ORDERED");
                put("$.configurations[0].maxParallel", 0);
                put("$.configurations[0].maxRetries", 0);
                put("$.configurations[0].retryDelayInMs", 0);
                put("$.configurations[0].ranking", 0);
                put("$.configurations[1].valid", false);
                put("$.configurations[1].type", "ORDERED");
                put("$.configurations[1].maxParallel", 0);
                put("$.configurations[1].maxRetries", 0);
                put("$.configurations[1].retryDelayInMs", 0);
                put("$.configurations[1].ranking", 0);
            }};

            expectedJsonPaths.forEach((k, v) -> {
                if (v != null) {
                    if (v instanceof String[]) {
                        for (String val : (String[]) v) {
                            assertThat(json, isJson(withJsonPath(k, hasItem(val))));
                        }
                    } else {
                        assertThat(json, hasJsonPath(k, equalTo(v)));
                    }
                } else {
                    assertThat(json, hasJsonPath(k));
                }
            });

        } catch (Exception e) {
            Assert.fail("Should not catch any exception. JSON format might be wrong. Error thrown: " + e.getMessage());
        }
    }
}
