package org.apache.druid.query.movingaverage.averagers;

import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.Map;

public class DoubleLastAverager extends BaseAverager<Number, Double>
{
    public DoubleLastAverager(int numBuckets, String name, String fieldName, int cycleSize)
    {
        super(Number.class, numBuckets, name, fieldName, cycleSize);
        startFrom = -1;
    }

    @Override
    protected Double computeResult()
    {
        int lastIndex = startFrom % numBuckets;
        return lastIndex != -1 && buckets[lastIndex] != null
                ? buckets[lastIndex].doubleValue()
                : Double.NEGATIVE_INFINITY;
    }

    @Override
    public void addElement(Map<String, Object> e, Map<String, AggregatorFactory> a) {
        super.addElement(e, a);
        startFrom++;
    }
}
