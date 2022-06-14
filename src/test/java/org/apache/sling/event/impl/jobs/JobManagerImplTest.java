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
package org.apache.sling.event.impl.jobs;

import static org.junit.Assert.assertEquals;

import org.apache.sling.event.jobs.JobManager.QueryType;
import org.junit.Test;

public class JobManagerImplTest {


    private static final String QUERY_ROOT = "/var/eventing/foobar";
    private static final QueryType QUERY_TYPE = QueryType.ACTIVE;
    
    // SLING-8413
    @Test
    public void testTopicEscaping() {
        String baseQuery = JobManagerImpl.buildBaseQuery(QUERY_ROOT, "randomNonQuotedTopic", QUERY_TYPE, false);
        assertEquals("/jcr:root/var/eventing/foobar/element(*,slingevent:Job)[@event.job.topic = "
                + "'randomNonQuotedTopic' and not(@slingevent:finishedState) and @event.job.started.time",baseQuery);

        String baseQuery2 = JobManagerImpl.buildBaseQuery(QUERY_ROOT, "random'Topic", QUERY_TYPE, false);
        assertEquals("/jcr:root/var/eventing/foobar/element(*,slingevent:Job)[@event.job.topic = "
                + "'random''Topic' and not(@slingevent:finishedState) and @event.job.started.time",baseQuery2);
    
    }
    
}
