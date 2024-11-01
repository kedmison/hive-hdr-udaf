package net.edmison.HdrHistogram.hive.udaf;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.HdrHistogram.Histogram;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.BeforeClass;
import org.junit.Test;

import net.edmison.HdrHistogram.hive.udaf.GenericUDAFHdrHistogram.HdrHistogramEvaluator;
import net.edmison.HdrHistogram.hive.udaf.GenericUDAFHdrHistogram.HdrHistogramEvaluatorBase.HdrHistogramAggregationBuffer;

/**
 * Unit test for simple App.
 */
public class TestGenericHdrHistogram {
    private static int numberofSignificantDigits = 3;
    private static Histogram baselineHistogram = new Histogram(numberofSignificantDigits);
    private static ArrayList<DoubleWritable[]> baselineDatapoints;

    private static TypeInfo[] funcParamTypes = new TypeInfo[] { TypeInfoFactory.longTypeInfo,
            TypeInfoFactory.intTypeInfo };
    private static ObjectInspector[] ois = new ObjectInspector[] {
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector };
    private static ObjectInspector[] partialOIs = new ObjectInspector[] {
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector};

    @BeforeClass
    public static void baselineHistogram() throws Exception {
        String sampleFileName = "src/test/resources/samples-1m.txt";
        BufferedReader reader = new BufferedReader(new FileReader(sampleFileName));
        String line;
        while ((line = reader.readLine()) != null) {
            baselineHistogram.recordValue(Long.parseLong(line));
        }
        reader.close();

        baselineDatapoints = HdrHistogramEvaluator.getDataPoints(baselineHistogram);
    }

    @Test
    public void testComplete() throws Exception {

        // init(complete)
        // call iterate, terminate.

        GenericUDAFHdrHistogram hdrHistogramCorrelator = new GenericUDAFHdrHistogram();
        GenericUDAFEvaluator eval = hdrHistogramCorrelator.getEvaluator(funcParamTypes);
        eval.init(GenericUDAFEvaluator.Mode.COMPLETE, ois);
        HdrHistogramAggregationBuffer aggBuf = (HdrHistogramAggregationBuffer) eval.getNewAggregationBuffer();

        String sampleFileName = "src/test/resources/samples-1m.txt";
        BufferedReader reader = new BufferedReader(new FileReader(sampleFileName));
        String line;
        Object[] iterationValues = new Object[2];
        iterationValues[1] = Integer.valueOf(numberofSignificantDigits); // fix the precision at 3 for this test

        while ((line = reader.readLine()) != null) {
            // record value in histogram
            iterationValues[0] = Long.parseLong(line);
            eval.iterate(aggBuf, iterationValues);
        }
        reader.close();

        Object histResult = eval.terminate(aggBuf);
        eval.close();

        assertEquals("Total rows processed", baselineHistogram.getTotalCount(), aggBuf.histogram.getTotalCount());
        validateResult(baselineDatapoints, histResult, "complete");
    }

    @Test
    public void testPartial1Final() throws Exception {
        // iterate in batches of 500
        int batchSize = 500;

        GenericUDAFHdrHistogram hdrHistogramCorrelator = new GenericUDAFHdrHistogram();
        GenericUDAFEvaluator eval = hdrHistogramCorrelator.getEvaluator(funcParamTypes);
        eval.init(GenericUDAFEvaluator.Mode.FINAL, ois);
        HdrHistogramAggregationBuffer aggBuf = (HdrHistogramAggregationBuffer) eval.getNewAggregationBuffer();

        String sampleFileName = "src/test/resources/samples-1m.txt";
        BufferedReader reader = new BufferedReader(new FileReader(sampleFileName));
    
        while (reader.ready()) {
            ArrayList<Long> partial1Batch = buildPartial1Batch(batchSize, reader);
            if (! partial1Batch.isEmpty()) {
                Object interimResult = runPartial1(partial1Batch);
                eval.merge(aggBuf, interimResult);
            } else {
                System.out.println("partial1Batch is empty");
            }
        }
        reader.close();

        Object endResult = eval.terminate(aggBuf);
        eval.close();

        assertEquals("Total rows processed", baselineHistogram.getTotalCount(), aggBuf.histogram.getTotalCount());
        validateResult(baselineDatapoints, endResult, "partial1final");
    }

    @Test
    public void testPartial1Partial2Final() throws Exception {
        // iterate in batches of 5000
        int batchSize = 5000;

        GenericUDAFHdrHistogram hdrHistogramCorrelator = new GenericUDAFHdrHistogram();
        GenericUDAFEvaluator eval = hdrHistogramCorrelator.getEvaluator(funcParamTypes);
        eval.init(GenericUDAFEvaluator.Mode.FINAL, partialOIs);
        HdrHistogramAggregationBuffer aggBuf = (HdrHistogramAggregationBuffer) eval.getNewAggregationBuffer();

        String sampleFileName = "src/test/resources/samples-1m.txt";
        BufferedReader reader = new BufferedReader(new FileReader(sampleFileName));
        Object[] iterationValues = new Object[2];
        iterationValues[1] = Integer.valueOf(numberofSignificantDigits); // fix the precision at 3 for this test

        while (reader.ready()) {
            ArrayList<Object> partial2Batch = buildPartial2Batch(batchSize, reader);
            if (! partial2Batch.isEmpty()) {
                Object interimResult = runPartial2(partial2Batch);
                eval.merge(aggBuf, interimResult);
            } else {
                System.out.println("partial2Batch is empty");
            }
        }
        reader.close();

        Object endResult = eval.terminate(aggBuf);
        eval.close();

        assertEquals("Total rows processed", baselineHistogram.getTotalCount(), aggBuf.histogram.getTotalCount());
        validateResult(baselineDatapoints, endResult, "partial1partial2final");
    }

    private ArrayList<Long> buildPartial1Batch(int batchSize, BufferedReader reader) throws IOException {
        ArrayList<Long> partialBatch = new ArrayList<Long>(batchSize);
        while (partialBatch.size() < batchSize) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            partialBatch.add(Long.parseLong(line));
        }
        return partialBatch;
    }

    private ArrayList<Object> buildPartial2Batch(int batchSize, BufferedReader reader) throws Exception {
        ArrayList<Object> partialBatch = new ArrayList<Object>(batchSize);
        while (partialBatch.size() < batchSize) {
            ArrayList<Long> partial1Batch = buildPartial1Batch(batchSize, reader);
            if (partial1Batch.isEmpty()) {
                // this batch is empty because buildPartial1Batch could not 
                // consume any more lines from the reader
                break;
            }
            Object interimResult = runPartial1(partial1Batch);
            partialBatch.add(interimResult);
        }
        return partialBatch;
    }


    private Object runPartial1(ArrayList<Long> batch) throws Exception {
        GenericUDAFHdrHistogram hdrHistogramCorrelator = new GenericUDAFHdrHistogram();
        GenericUDAFEvaluator eval = hdrHistogramCorrelator.getEvaluator(funcParamTypes);
        eval.init(GenericUDAFEvaluator.Mode.PARTIAL1, ois);
        AggregationBuffer aggBuf = eval.getNewAggregationBuffer();

        Object[] iterationValues = new Object[2];
        iterationValues[1] = Integer.valueOf(numberofSignificantDigits); // fix the precision at 3 for this test

        for (Iterator<Long> iter = batch.iterator(); iter.hasNext();) {
            iterationValues[0] = iter.next();
            eval.iterate(aggBuf, iterationValues);
        }
        Object result = eval.terminatePartial(aggBuf);
        eval.close();
        return result;
    }


    private Object runPartial2(ArrayList<Object> batch) throws Exception {
        GenericUDAFHdrHistogram hdrHistogramCorrelator = new GenericUDAFHdrHistogram();
        GenericUDAFEvaluator eval = hdrHistogramCorrelator.getEvaluator(funcParamTypes);
        eval.init(GenericUDAFEvaluator.Mode.PARTIAL2, partialOIs);
        AggregationBuffer aggBuf = eval.getNewAggregationBuffer();


        for (Iterator<Object> iter = batch.iterator(); iter.hasNext();) {
            Object interimResult = iter.next();
            eval.merge(aggBuf, interimResult);
        }
        Object result = eval.terminatePartial(aggBuf);
        eval.close();
        return result;
    }


    @SuppressWarnings("unchecked")
    private void validateResult(ArrayList<DoubleWritable[]> baselineDatapoints, Object actual, String filePrefix) {

        assertNotNull("Baseline Datapoints are null", baselineDatapoints);
        assertFalse("Baseline Datapoints are empty", baselineDatapoints.isEmpty());

        assertNotNull("Calculated Datapoints are null", actual);
        assertTrue("Calculated Datapoints not the expected datatype", actual instanceof ArrayList<?>);
        ArrayList<DoubleWritable[]> actualDatapoints = (ArrayList<DoubleWritable[]>) actual;
        assertFalse("Calculated Datapoints are empty", actualDatapoints.isEmpty());
        assertArrayEquals("Baseline and calculated data points differ", baselineDatapoints.toArray(new DoubleWritable[0][0]),
                actualDatapoints.toArray(new DoubleWritable[0][0]));
    }

}
