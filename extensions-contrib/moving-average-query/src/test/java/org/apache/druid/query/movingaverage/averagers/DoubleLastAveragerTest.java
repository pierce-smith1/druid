package org.apache.druid.query.movingaverage.averagers;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

public class DoubleLastAveragerTest {
    @Test
    public void testComputeResult() {
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
