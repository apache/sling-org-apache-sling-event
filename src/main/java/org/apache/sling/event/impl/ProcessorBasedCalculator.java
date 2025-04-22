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
package org.apache.sling.event.impl;

import org.apache.sling.event.impl.jobs.config.ConfigurationConstants;
import org.slf4j.Logger;

public abstract class ProcessorBasedCalculator {

    /**
     * Float values are treated as percentage.  int values are treated as number of cores, -1 == all available
     * Note: the value is based on the core count at startup.  It will not change dynamically if core count changes.
     *
     * @param input The input, being either -1, between 0 and 1 or 1+
     * @return input of -1 will return the amount of cores, between 0 and 1 will return a percentage of the cores, 1 or higher returns the same number
     */
    public static int calculate(final Logger logger, final double input) {
        int cores = ConfigurationConstants.NUMBER_OF_PROCESSORS;
        int result = cores;
        logger.debug("Input for processor based calculation {}", input);
        if ((input == Math.floor(input)) && !Double.isInfinite(input)) {
            // integral type
            if ((int) input == 0) {
                logger.warn("Input set to zero.");
            }
            result = (input <= -1 ? cores : (int) input);
        } else {
            // percentage (rounded)
            if ((input > 0.0) && (input < 1.0)) {
                result = (int) Math.round(cores * input);
            } else {
                logger.warn("Invalid input {}, defaulting to cores {}.", input, cores);
            }
        }
        logger.debug("Result {}", result);
        return result;
    }
}
