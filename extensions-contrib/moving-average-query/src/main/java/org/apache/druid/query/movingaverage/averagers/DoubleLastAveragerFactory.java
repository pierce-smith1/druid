package org.apache.druid.query.movingaverage.averagers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.column.ValueType;

public class DoubleLastAveragerFactory extends ComparableAveragerFactory<Double, Double>
{

    @JsonCreator
    public DoubleLastAveragerFactory(
            @JsonProperty("name") String name,
            @JsonProperty("buckets") int numBuckets,
            @JsonProperty("cycleSize") Integer cycleSize,
            @JsonProperty("fieldName") String fieldName
    )
    {
        super(name, numBuckets, fieldName, cycleSize);
    }

    @Override
    public Averager<Double> createAverager()
    {
        return new DoubleLastAverager(numBuckets, name, fieldName, cycleSize);
    }

    @Override
    public ValueType getType()
    {
        return ValueType.DOUBLE;
    }
}
