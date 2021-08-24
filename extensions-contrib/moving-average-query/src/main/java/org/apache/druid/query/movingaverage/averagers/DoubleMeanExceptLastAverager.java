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
            int bucketIndex = (i + startFrom + 2) % numBuckets;
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
    public void addElement(Map<String, Object> e, Map<String, AggregatorFactory> a) {
        super.addElement(e, a);
        startFrom++;
    }
}
