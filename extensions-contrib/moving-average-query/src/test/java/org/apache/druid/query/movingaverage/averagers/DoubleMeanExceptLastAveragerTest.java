package org.apache.druid.query.movingaverage.averagers;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

public class DoubleMeanExceptLastAveragerTest
{
    @Test
    public void testComputeResult()
    {
        HashMap<String, AggregatorFactory> dummy = new HashMap<>();
        BaseAverager<Number, Double> avgr = new DoubleMeanExceptLastAverager(4, "test", "field", 1);
        Assert.assertEquals(0.0, avgr.computeResult(), 0.0);

        avgr.addElement(Collections.singletonMap("field", 0.0), dummy);
        Assert.assertEquals(0.0, avgr.computeResult(), 0.0);

        avgr.addElement(Collections.singletonMap("field", 2.0), dummy);
        Assert.assertEquals(0.0, avgr.computeResult(), 0.0);

        avgr.addElement(Collections.singletonMap("field", 2.0), dummy);
        Assert.assertEquals(2.0 / 3, avgr.computeResult(), 0.1);

        avgr.addElement(Collections.singletonMap("field", 2.0), dummy);
        Assert.assertEquals(4.0 / 3, avgr.computeResult(), 0.1);

        avgr.addElement(Collections.singletonMap("field", 30.0), dummy);
        avgr.addElement(Collections.singletonMap("field", 20.0), dummy);
        Assert.assertEquals((30.0 + 20.0 + 2.0) / 3, avgr.computeResult(), 0.1);

    }
}
