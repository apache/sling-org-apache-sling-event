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
import org.apache.sling.event.jobs.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Helper class that holds gauges for relevant queue statistics.
 */
class GaugeSupport {

    static final String GAUGE_NAME_PREFIX = "event.jobs";
    static final String QUEUE_PREFIX = "queue";
    static final String CANCELLED_METRIC_SUFFIX = ".cancelled.count";
    static final String FINISHED_METRIC_SUFFIX = ".finished.count";
    static final String FAILED__METRIC_SUFFIX = ".failed.count";
    static final String QUEUED_METRIC_SUFFIX = ".queued.count";
    static final String PROCESSED_METRIC_SUFFIX = ".processed.count";
    static final String ACTIVE_METRIC_SUFFIX = ".active.count";
    static final String AVG_WAITING_TIME_METRIC_SUFFIX = ".averageWaitingTime";
    static final String AVG_PROCESSING_TIME_METRIC_SUFFIX = ".averageProcessingTime";

    private final MetricRegistry metricRegistry;
    private final Map<String, Gauge<Long>> gaugeList = new HashMap<>();
    private final Set<String> gaugeMetricNames = new HashSet<>();
    private final String queueName;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Create a new GaugeSupport instance for the global queue.
     *
     * @param globalQueueStats the global queueStats
     * @param metricRegistry   the (sling) metric registry
     */
    GaugeSupport(final Statistics globalQueueStats, final MetricRegistry metricRegistry) {
        this(null, globalQueueStats, metricRegistry);
    }

    /**
     * Creates a new GaugeSupport instance. Registers gauges for jobs (based on the queueStats).
     *
     * @param queueName      name of the queue
     * @param queueStats     queueStats of that queue
     * @param metricRegistry the (sling) metric registry
     */
    GaugeSupport(final String queueName, final Statistics queueStats, final MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        this.queueName = getSanitizedQueueName(queueName);
        if (metricRegistry != null && queueStats != null) {
            gaugeList.put(FINISHED_METRIC_SUFFIX, new Gauge<Long>() {

                public Long getValue() {
                    return queueStats.getNumberOfFinishedJobs();
                }
            });
            gaugeList.put(CANCELLED_METRIC_SUFFIX, new Gauge<Long>() {

                public Long getValue() {
                    return queueStats.getNumberOfCancelledJobs();
                }
            });
            gaugeList.put(FAILED__METRIC_SUFFIX, new Gauge<Long>() {

                public Long getValue() {
                    return queueStats.getNumberOfFailedJobs();
                }
            });
            gaugeList.put(QUEUED_METRIC_SUFFIX, new Gauge<Long>() {

                public Long getValue() {
                    return queueStats.getNumberOfQueuedJobs();
                }
            });
            gaugeList.put(PROCESSED_METRIC_SUFFIX, new Gauge<Long>() {

                public Long getValue() {
                    return queueStats.getNumberOfProcessedJobs();
                }
            });
            gaugeList.put(ACTIVE_METRIC_SUFFIX, new Gauge<Long>() {

                public Long getValue() {
                    return queueStats.getNumberOfActiveJobs();
                }
            });
            gaugeList.put(AVG_WAITING_TIME_METRIC_SUFFIX, new Gauge<Long>() {

                public Long getValue() {
                    return queueStats.getAverageWaitingTime();
                }
            });
            gaugeList.put(AVG_PROCESSING_TIME_METRIC_SUFFIX, new Gauge<Long>() {

                public Long getValue() {
                    return queueStats.getAverageProcessingTime();
                }
            });
        }
    }

    /**
     * Unregisters all job gauges of the queue.
     */
    void shutdown() {
        if (metricRegistry != null) {
           for (String metricName : gaugeMetricNames) {
                try {
                    metricRegistry.remove(metricName);
                } catch (RuntimeException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Initializes the metric registry with the gauges.
     */
    void initialize() {
        for (Map.Entry<String, Gauge<Long>> entry : gaugeList.entrySet()) {
            registerWithSuffix(entry.getKey(), 0, entry.getValue());
        }
    }

    private void registerWithSuffix(String suffix, int count, Gauge<Long> value) {
        try {
            String metricName = getMetricName(queueName, count, suffix);
            metricRegistry.register(metricName, value);
            gaugeMetricNames.add(metricName);
        } catch (IllegalArgumentException e) {
            if (queueName != null && count <= 10) {
                registerWithSuffix(suffix, count + 1, value);
            } else {
                logger.debug("Failed to register suffix {} for the queue {}, attempt {}", suffix, queueName, count, e);
            }
        }
    }

    private String getMetricName(final String queueName, int count, final String metricSuffix) {
        String metricName = (queueName != null ? "." + QUEUE_PREFIX + "." + queueName : "");
        if (count > 0) {
            metricName = metricName + "_" + count;
        }
        return GAUGE_NAME_PREFIX + metricName + metricSuffix;
    }

    private String getSanitizedQueueName(String queueName) {
        if (queueName == null) {
            return null;
        }
        return queueName.replaceAll("[^a-zA-Z\\d]", "_").toLowerCase();
    }
}
