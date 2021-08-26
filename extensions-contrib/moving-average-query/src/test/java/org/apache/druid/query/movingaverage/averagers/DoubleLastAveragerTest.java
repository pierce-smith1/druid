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

package org.apache.druid.query.movingaverage.averagers;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

public class DoubleLastAveragerTest
{
  @Test
  public void testComputeResult()
  {
    HashMap<String, AggregatorFactory> dummy = new HashMap<>();
    BaseAverager<Number, Double> avgr = new DoubleLastAverager(3, "test", "field", 1);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, avgr.computeResult(), 0.0);

    avgr.addElement(Collections.singletonMap("field", 0.0), dummy);
    Assert.assertEquals(0.0, avgr.computeResult(), 0.0);

    avgr.addElement(Collections.singletonMap("field", 1.0), dummy);
    avgr.addElement(Collections.singletonMap("field", 1.5), dummy);
    Assert.assertEquals(1.5, avgr.computeResult(), 0.0);

    avgr.addElement(Collections.singletonMap("field", 2.0), dummy);
    Assert.assertEquals(2.0, avgr.computeResult(), 0.0);

    avgr.addElement(Collections.singletonMap("field", 2.5), dummy);
    Assert.assertEquals(2.5, avgr.computeResult(), 0.0);

    avgr.addElement(Collections.singletonMap("field", 3.0), dummy);
    avgr.addElement(Collections.singletonMap("field", 3.5), dummy);
    Assert.assertEquals(3.5, avgr.computeResult(), 0.0);
  }
}
