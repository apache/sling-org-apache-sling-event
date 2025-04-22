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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class ProcessorBasedCalculatorTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testCalculate() {
        assertEquals(Runtime.getRuntime().availableProcessors(), ProcessorBasedCalculator.calculate(logger, -1));

        // Edge cases 0.0 and 1.0 (treated as int numbers)
        assertEquals(0, ProcessorBasedCalculator.calculate(logger, 0.0));

        assertEquals(1, ProcessorBasedCalculator.calculate(logger, 1.0));

        // percentage (50%)
        assertEquals((int) Math.round(Runtime.getRuntime().availableProcessors() * 0.5), ProcessorBasedCalculator.calculate(logger, 0.5));

        // rounding
        assertEquals((int) Math.round(Runtime.getRuntime().availableProcessors() * 0.9), ProcessorBasedCalculator.calculate(logger, 0.90));

        assertEquals((int) Math.round(Runtime.getRuntime().availableProcessors() * 0.99), ProcessorBasedCalculator.calculate(logger, 0.99));

        // Percentages can't go over 99% (0.99)
        assertEquals(Runtime.getRuntime().availableProcessors(), ProcessorBasedCalculator.calculate(logger, 1.01));

        // Treat negative values same a -1 (all cores)
        assertEquals(Runtime.getRuntime().availableProcessors(), ProcessorBasedCalculator.calculate(logger, -0.5));

        assertEquals(Runtime.getRuntime().availableProcessors(), ProcessorBasedCalculator.calculate(logger, -2));
    }
}
