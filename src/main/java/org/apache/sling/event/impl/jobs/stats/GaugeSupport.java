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

import java.util.HashMap;
import java.util.Map;

/** Helper class that holds gauges for relevant queue statistics. */
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
    private Map<String, Gauge> gaugeList = new HashMap<>();

    /** Create a new GaugeSupport instance for the global queue.
     *
     * @param globalQueueStats the global queueStats
     * @param metricRegistry the (sling) metric registry */
    GaugeSupport(final Statistics globalQueueStats, MetricRegistry metricRegistry) {
        this(null, globalQueueStats, metricRegistry);
    }

    /** Creates a new GaugeSupport instance. Registers gauges for jobs (based on the queueStats).
     *
     * @param queueName name of the queue
     * @param queueStats queueStats of that queue
     * @param metricRegistry the (sling) metric registry */
    GaugeSupport(String queueName, final Statistics queueStats, MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        if (metricRegistry != null) {
            gaugeList.put(getMetricName(queueName, FINISHED_METRIC_SUFFIX), new Gauge() {

                public Object getValue() {
                    return queueStats.getNumberOfFinishedJobs();
                }
            });
            gaugeList.put(getMetricName(queueName, CANCELLED_METRIC_SUFFIX), new Gauge() {

                public Object getValue() {
                    return queueStats.getNumberOfCancelledJobs();
                }
            });
            gaugeList.put(getMetricName(queueName, FAILED__METRIC_SUFFIX), new Gauge() {

                public Object getValue() {
                    return queueStats.getNumberOfFailedJobs();
                }
            });
            gaugeList.put(getMetricName(queueName, QUEUED_METRIC_SUFFIX), new Gauge() {

                public Object getValue() {
                    return queueStats.getNumberOfQueuedJobs();
                }
            });
            gaugeList.put(getMetricName(queueName, PROCESSED_METRIC_SUFFIX), new Gauge() {

                public Object getValue() {
                    return queueStats.getNumberOfProcessedJobs();
                }
            });
            gaugeList.put(getMetricName(queueName, ACTIVE_METRIC_SUFFIX), new Gauge() {

                public Object getValue() {
                    return queueStats.getNumberOfActiveJobs();
                }
            });
            gaugeList.put(getMetricName(queueName, AVG_WAITING_TIME_METRIC_SUFFIX), new Gauge() {

                public Object getValue() {
                    return queueStats.getAverageWaitingTime();
                }
            });
            gaugeList.put(getMetricName(queueName, AVG_PROCESSING_TIME_METRIC_SUFFIX), new Gauge() {

                public Object getValue() {
                    return queueStats.getAverageProcessingTime();
                }
            });
            for (Map.Entry<String, Gauge> entry : gaugeList.entrySet()) {
                metricRegistry.register(entry.getKey(), entry.getValue());
            }
        }
    }

    private String getMetricName(String queueName, String metricSuffix) {
        return GAUGE_NAME_PREFIX + (queueName != null ? "." + QUEUE_PREFIX + "." + queueName : "") + metricSuffix;
    }

    /** Unregisters all job gauges of the queue. */
    void shutdown() {
        if (metricRegistry != null) {
            for (String metricName : gaugeList.keySet()) {
                metricRegistry.remove(metricName);
            }
        }
    }

}
