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
package org.apache.sling.event.impl.jobs.stats;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.sling.event.impl.TestUtil;
import org.apache.sling.event.impl.jobs.InternalJobState;
import org.apache.sling.event.jobs.Statistics;
import org.junit.Before;
import org.junit.Test;

import static org.apache.sling.event.impl.jobs.stats.GaugeSupport.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StatisticsManagerTest {

    private static final String TEST_QUEUE_NAME = "queue_name";
    private static final String TEST_TOPIC = "testtopic";

    private StatisticsManager statisticsManager;
    private MetricRegistry metricRegistry;

    private StatisticsImpl statistics = new StatisticsImpl();

    @Before
    public void setup() {
        statisticsManager = new StatisticsManager();
        metricRegistry = new MetricRegistry();
        TestUtil.setFieldValue(statisticsManager, "globalStatistics", statistics);
        TestUtil.setFieldValue(statisticsManager, "metricRegistry", metricRegistry);
        statisticsManager.activate();
    }

    @Test
    public void testActivateDeactivateWithMissingRegistry() {
        statisticsManager = new StatisticsManager();
        TestUtil.setFieldValue(statisticsManager, "globalStatistics", statistics);
        statisticsManager.activate();
        assertNull(TestUtil.getFieldValue(statisticsManager, "metricRegistry"));
        statisticsManager.deactivate();
    }

    @Test
    public void testGlobalGaugesAreRemovedOnDeactivate() {
        statisticsManager.jobQueued(TEST_QUEUE_NAME, TEST_TOPIC);
        assertEquals("Less than 16 metrics present (8 global + 8 topic).",
                16, metricRegistry.getMetrics().size());
        statisticsManager.deactivate();
        assertEquals(0, metricRegistry.getMetrics().size());
    }

    @Test
    public void testJobStarted() {
        long queueTime = 10L;
        statisticsManager.jobStarted(TEST_QUEUE_NAME, TEST_TOPIC, queueTime);
        Gauge topicMetric = getTopicMetric(ACTIVE_METRIC_SUFFIX);
        Gauge waitingTimeTopicMetric = getTopicMetric(AVG_WAITING_TIME_METRIC_SUFFIX);
        Gauge globalMetric = getGlobalMetric(ACTIVE_METRIC_SUFFIX);
        Gauge waitingTimeGlobalMetric = getGlobalMetric(AVG_WAITING_TIME_METRIC_SUFFIX);

        Statistics queueStatistics = statisticsManager.getQueueStatistics(TEST_QUEUE_NAME);

        assertEquals(1L, queueStatistics.getNumberOfActiveJobs());
        assertEquals((Long) 1L, topicMetric.getValue());
        assertEquals((Long) 1L, globalMetric.getValue());
        assertEquals((Long) queueStatistics.getAverageWaitingTime(), waitingTimeTopicMetric.getValue());
        assertEquals((Long) queueStatistics.getAverageWaitingTime(), waitingTimeGlobalMetric.getValue());
    }

    @Test
    public void testJobQueueDequeue() {
        statisticsManager.jobQueued(TEST_QUEUE_NAME, TEST_TOPIC);
        Gauge topicMetric = getTopicMetric(QUEUED_METRIC_SUFFIX);
        Gauge globalMetric = getGlobalMetric(QUEUED_METRIC_SUFFIX);
        Statistics queueStatistics = statisticsManager.getQueueStatistics(TEST_QUEUE_NAME);
        assertEquals(1L, queueStatistics.getNumberOfQueuedJobs());
        assertEquals((Long) 1L, topicMetric.getValue());
        assertEquals((Long) 1L, globalMetric.getValue());

        statisticsManager.jobDequeued(TEST_QUEUE_NAME, TEST_TOPIC);
        assertEquals(0L, queueStatistics.getNumberOfQueuedJobs());
        assertEquals((Long) 0L, topicMetric.getValue());
        assertEquals((Long) 0L, globalMetric.getValue());
    }

    @Test
    public void testJobCancelled() {
        statisticsManager.jobEnded(TEST_QUEUE_NAME, TEST_TOPIC, InternalJobState.CANCELLED, 0L);
        Gauge topicMetric = getTopicMetric(CANCELLED_METRIC_SUFFIX);
        Gauge globalMetric = getGlobalMetric(CANCELLED_METRIC_SUFFIX);
        Statistics queueStatistics = statisticsManager.getQueueStatistics(TEST_QUEUE_NAME);
        assertEquals(1L, queueStatistics.getNumberOfCancelledJobs());
        assertEquals((Long) 1L, topicMetric.getValue());
        assertEquals((Long) 1L, globalMetric.getValue());
    }

    @Test
    public void testJobFailed() {
        statisticsManager.jobEnded(TEST_QUEUE_NAME, TEST_TOPIC, InternalJobState.FAILED, 0L);
        Gauge topicMetric = getTopicMetric(FAILED__METRIC_SUFFIX);
        Gauge globalMetric = getGlobalMetric(FAILED__METRIC_SUFFIX);
        Statistics queueStatistics = statisticsManager.getQueueStatistics(TEST_QUEUE_NAME);
        assertEquals(1L, queueStatistics.getNumberOfFailedJobs());
        assertEquals((Long) 1L, topicMetric.getValue());
        assertEquals((Long) 1L, globalMetric.getValue());
    }

    @Test
    public void testJobFinished() {
        long processingTime = 10L;
        statisticsManager.jobEnded(TEST_QUEUE_NAME, TEST_TOPIC, InternalJobState.SUCCEEDED, processingTime);
        Gauge finishedTopicMetric = getTopicMetric(FINISHED_METRIC_SUFFIX);
        Gauge processedTopicMetric = getTopicMetric(PROCESSED_METRIC_SUFFIX);
        Gauge processingTopicTimeMetric = getTopicMetric(AVG_PROCESSING_TIME_METRIC_SUFFIX);
        Gauge finishedGlobalMetric = getGlobalMetric(FINISHED_METRIC_SUFFIX);
        Gauge processedGlobalMetric = getGlobalMetric(PROCESSED_METRIC_SUFFIX);
        Gauge processingGlobalTimeMetric = getGlobalMetric(AVG_PROCESSING_TIME_METRIC_SUFFIX);

        Statistics queueStatistics = statisticsManager.getQueueStatistics(TEST_QUEUE_NAME);

        assertEquals(1L, queueStatistics.getNumberOfFinishedJobs());
        assertEquals((Long) 1L, finishedTopicMetric.getValue());
        assertEquals((Long) 1L, processedTopicMetric.getValue());
        assertEquals((Long) queueStatistics.getAverageProcessingTime(), processingTopicTimeMetric.getValue());
        assertEquals((Long) 1L, finishedGlobalMetric.getValue());
        assertEquals((Long) 1L, processedGlobalMetric.getValue());
        assertEquals((Long) queueStatistics.getAverageProcessingTime(), processingGlobalTimeMetric.getValue());
    }

    @Test
    public void testQueueWithSpecialCharsIsSanitized() {
        String queueName = "Topic*With%Special/Chars";
        String queueName2 = "topic$with?special§chars";
        String queueName3 = "topic with<special>chars";
        statisticsManager.jobQueued(queueName, TEST_TOPIC);
        statisticsManager.jobQueued(queueName2, TEST_TOPIC);
        statisticsManager.jobQueued(queueName3, TEST_TOPIC);

        Gauge topicMetric = (Gauge) metricRegistry.getMetrics().get(GAUGE_NAME_PREFIX +
                "." + QUEUE_PREFIX + ".topic_with_special_chars" + QUEUED_METRIC_SUFFIX);
        Gauge topicMetric2 = (Gauge) metricRegistry.getMetrics().get(GAUGE_NAME_PREFIX +
                "." + QUEUE_PREFIX + ".topic_with_special_chars_1" + QUEUED_METRIC_SUFFIX);
        Gauge topicMetric3 = (Gauge) metricRegistry.getMetrics().get(GAUGE_NAME_PREFIX +
                "." + QUEUE_PREFIX + ".topic_with_special_chars_2" + QUEUED_METRIC_SUFFIX);
        assertEquals(1L, topicMetric.getValue());
        assertEquals(1L, topicMetric2.getValue());
        assertEquals(1L, topicMetric3.getValue());
    }

    private Gauge getTopicMetric(String metricSuffix) {
        return (Gauge) metricRegistry.getMetrics().get(GAUGE_NAME_PREFIX +
                "." + QUEUE_PREFIX + "." + TEST_QUEUE_NAME + metricSuffix);
    }

    private Gauge getGlobalMetric(String metricSuffix) {
        return (Gauge) metricRegistry.getMetrics().get(GAUGE_NAME_PREFIX + metricSuffix);
    }


}