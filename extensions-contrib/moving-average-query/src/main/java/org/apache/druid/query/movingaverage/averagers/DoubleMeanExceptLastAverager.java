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

import java.util.Map;

public class DoubleMeanExceptLastAverager extends BaseAverager<Number, Double>
{
  public DoubleMeanExceptLastAverager(int numBuckets, String name, String fieldName, int cycleSize)
  {
    super(Number.class, numBuckets, name, fieldName, cycleSize);
  }

  @Override
  protected Double computeResult()
  {
    double result = 0.0;
    int validBuckets = 0;

    for (int i = 0; i < numBuckets - 1; i += cycleSize) {
      int bucketIndex = (i + startFrom) % numBuckets;
      if (buckets[bucketIndex] != null) {
        result += (buckets[bucketIndex]).doubleValue();
      } else {
        result += 0.0;
      }
      validBuckets++;
    }

    return result / validBuckets;
  }

  @Override
  public void addElement(Map<String, Object> e, Map<String, AggregatorFactory> a)
  {
    super.addElement(e, a);
    startFrom++;
  }
}
