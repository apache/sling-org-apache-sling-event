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

import static org.junit.Assert.assertEquals;

public class InventoryPluginTest extends Mockito {
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
        MockitoAnnotations.initMocks(this);

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

        String output = writerOutput.toString();
        assertEquals("Apache Sling Job Handling\n" +
                "-------------------------\n" +
                "Overall Statistics\n" +
                "Start Time : 01:00:00:000 1970-Jan-01\n" +
                "Local topic consumers: \n" +
                "Last Activated : 01:00:00:000 1970-Jan-01\n" +
                "Last Finished : 01:00:00:000 1970-Jan-01\n" +
                "Queued Jobs : 0\n" +
                "Active Jobs : 0\n" +
                "Jobs : 0\n" +
                "Finished Jobs : 0\n" +
                "Failed Jobs : 0\n" +
                "Cancelled Jobs : 0\n" +
                "Processed Jobs : 0\n" +
                "Average Processing Time : -\n" +
                "Average Waiting Time : -\n" +
                "\n" +
                "Topology Capabilities\n" +
                "No topology information available !\n" +
                "Scheduled Jobs\n" +
                "Schedule\n" +
                "Job Topic< : topic\n" +
                "Schedules : HOURLY 40, CRON 0 * * * *\n\n\n" +
                "Active JobQueue: null \n" +
                "Statistics\n" +
                "Start Time : 01:00:00:000 1970-Jan-01\n" +
                "Last Activated : 01:00:00:000 1970-Jan-01\n" +
                "Last Finished : 01:00:00:000 1970-Jan-01\n" +
                "Queued Jobs : 0\n" +
                "Active Jobs : 0\n" +
                "Jobs : 0\n" +
                "Finished Jobs : 0\n" +
                "Failed Jobs : 0\n" +
                "Cancelled Jobs : 0\n" +
                "Processed Jobs : 0\n" +
                "Average Processing Time : -\n" +
                "Average Waiting Time : -\n" +
                "Status Info : null\n" +
                "Configuration\n" +
                "Type : Ordered\n" +
                "Topics : \n" +
                "Max Parallel : 0\n" +
                "Max Retries : 0\n" +
                "Retry Delay : 0 ms\n" +
                "Priority : null\n" +
                "\n" +
                "Topic Statistics - null\n" +
                "Last Activated : 01:00:00:000 1970-Jan-01\n" +
                "Last Finished : 01:00:00:000 1970-Jan-01\n" +
                "Finished Jobs : 0\n" +
                "Failed Jobs : 0\n" +
                "Cancelled Jobs : 0\n" +
                "Processed Jobs : 0\n" +
                "Average Processing Time : -\n" +
                "Average Waiting Time : -\n" +
                "\n" +
                "Apache Sling Job Handling - Job Queue Configurations\n" +
                "----------------------------------------------------\n" +
                "Job Queue Configuration: null\n" +
                "Valid : false\n" +
                "Type : Ordered\n" +
                "Topics : \n" +
                "Max Parallel : 0\n" +
                "Max Retries : 0\n" +
                "Retry Delay : 0 ms\n" +
                "Priority : null\n" +
                "Ranking : 0\n" +
                "\n" +
                "Job Queue Configuration: null\n" +
                "Valid : false\n" +
                "Type : Ordered\n" +
                "Topics : \n" +
                "Max Parallel : 0\n" +
                "Max Retries : 0\n" +
                "Retry Delay : 0 ms\n" +
                "Priority : null\n" +
                "Ranking : 0\n\n", output);
    }

    @Test
    public void printJsonFormat() {
        StringWriter writerOutput = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writerOutput);

        inventoryPlugin.print(printWriter, Format.JSON, false);

        String output = writerOutput.toString();
        assertEquals("{\n" +
                "  \"statistics\" : {\n" +
                "    \"startTime\" : 0,\n" +
                "    \"startTimeText\" : \"01:00:00:000 1970-Jan-01\",\n" +
                "    \"lastActivatedJobTime\" : 0,\n" +
                "    \"lastActivatedJobTimeText\" : \"01:00:00:000 1970-Jan-01\",\n" +
                "    \"lastFinishedJobTime\" : 0,\n" +
                "    \"lastFinishedJobTimeText\" : \"01:00:00:000 1970-Jan-01\",\n" +
                "    \"numberOfQueuedJobs\" : 0,\n" +
                "    \"numberOfActiveJobs\" : 0,\n" +
                "    \"numberOfJobs\" : 0,\n" +
                "    \"numberOfFinishedJobs\" : 0,\n" +
                "    \"numberOfFailedJobs\" : 0,\n" +
                "    \"numberOfCancelledJobs\" : 0,\n" +
                "    \"numberOfProcessedJobs\" : 0,\n" +
                "    \"averageProcessingTime\" : 0,\n" +
                "    \"averageProcessingTimeText\" : \"-\",\n" +
                "    \"averageWaitingTime\" : 0,\n" +
                "    \"averageWaitingTimeText\" : \"-\"\n" +
                "  },\n" +
                "  \"scheduledJobs\" : [\n" +
                "    {\n" +
                "      \"jobTopic\" : \"topic\",\n" +
                "      \"schedules\" : [\n" +
                "        {\n" +
                "          \"type\" : \"HOURLY\",\n" +
                "          \"schedule\" : \"40\"\n" +
                "        }, \n" +
                "        {\n" +
                "          \"type\" : \"CRON\",\n" +
                "          \"schedule\" : \"0 * * * *\"\n" +
                "        }      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"queues\" : [\n" +
                "    {\n" +
                "      \"name\" : \"null\",\n" +
                "      \"suspended\" : false,\n" +
                "      \"statistics\" : {\n" +
                "        \"startTime\" : 0,\n" +
                "        \"startTimeText\" : \"01:00:00:000 1970-Jan-01\",\n" +
                "        \"lastActivatedJobTime\" : 0,\n" +
                "        \"lastActivatedJobTimeText\" : \"01:00:00:000 1970-Jan-01\",\n" +
                "        \"lastFinishedJobTime\" : 0,\n" +
                "        \"lastFinishedJobTimeText\" : \"01:00:00:000 1970-Jan-01\",\n" +
                "        \"numberOfQueuedJobs\" : 0,\n" +
                "        \"numberOfActiveJobs\" : 0,\n" +
                "        \"numberOfJobs\" : 0,\n" +
                "        \"numberOfFinishedJobs\" : 0,\n" +
                "        \"numberOfFailedJobs\" : 0,\n" +
                "        \"numberOfCancelledJobs\" : 0,\n" +
                "        \"numberOfProcessedJobs\" : 0,\n" +
                "        \"averageProcessingTime\" : 0,\n" +
                "        \"averageProcessingTimeText\" : \"-\",\n" +
                "        \"averageWaitingTime\" : 0,\n" +
                "        \"averageWaitingTimeText\" : \"-\"\n" +
                "      },      \"stateInfo\" : \"null\",\n" +
                "      \"configuration\" : {\n" +
                "        \"type\" : \"ORDERED\",\n" +
                "        \"topics\" : \"[]\",\n" +
                "        \"maxParallel\" : 0,\n" +
                "        \"maxRetries\" : 0,\n" +
                "        \"retryDelayInMs\" : 0,\n" +
                "        \"priority\" : \"null\"\n" +
                "      }\n" +
                "    }  ],\n" +
                "  \"topicStatistics\" : [\n" +
                "    {\n" +
                "      \"topic\" : \"null\",\n" +
                "      \"lastActivatedJobTime\" : 0,\n" +
                "      \"lastActivatedJobTimeText\" : \"01:00:00:000 1970-Jan-01\",\n" +
                "      \"lastFinishedJobTime\" : 0,\n" +
                "      \"lastFinishedJobTimeText\" : \"01:00:00:000 1970-Jan-01\",\n" +
                "      \"numberOfFinishedJobs\" : 0,\n" +
                "      \"numberOfFailedJobs\" : 0,\n" +
                "      \"numberOfCancelledJobs\" : 0,\n" +
                "      \"numberOfProcessedJobs\" : 0,\n" +
                "      \"averageProcessingTime\" : 0,\n" +
                "      \"averageProcessingTimeText\" : \"-\",\n" +
                "      \"averageWaitingTime\" : 0,\n" +
                "      \"averageWaitingTimeText\" : \"-\"\n" +
                "    }  ],\n" +
                "  \"configurations\" : [\n" +
                "    {\n" +
                "      \"name\" : \"null\",\n" +
                "      \"valid\" : false,\n" +
                "      \"type\" : \"ORDERED\",\n" +
                "      \"topics\" : [],\n" +
                "      \"maxParallel\" : 0,\n" +
                "      \"maxRetries\" : 0,\n" +
                "      \"retryDelayInMs\" : 0,\n" +
                "      \"priority\" : \"null\",\n" +
                "      \"ranking\" : 0\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\" : \"null\",\n" +
                "      \"valid\" : false,\n" +
                "      \"type\" : \"ORDERED\",\n" +
                "      \"topics\" : [],\n" +
                "      \"maxParallel\" : 0,\n" +
                "      \"maxRetries\" : 0,\n" +
                "      \"retryDelayInMs\" : 0,\n" +
                "      \"priority\" : \"null\",\n" +
                "      \"ranking\" : 0\n" +
                "    }\n" +
                "  ]\n" +
                "}\n", output);
    }
}
