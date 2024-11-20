package net.edmison.HdrHistogram.hive.udaf;

import java.nio.ByteBuffer;
import org.HdrHistogram.AbstractHistogram;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.io.Text;

/**
 * HdrHistogramEncoderEvaluatorBase is an abstract class that extends HdrHistogramEvaluatorBase, 
 * which provides most of the implementation needed for aggregating HDR Histograms as an Apache Hive UDAF.
 * 
 * This class does not provides an abstract recordValue method, so it must be implemented by subclasses
 * to handle the data type of the column being aggregated.
 * 
 * This method provides a getResult method that returns the aggregated HDR Histogram as 
 * a base64-encoded string.
 */


public abstract class HdrHistogramEncoderEvaluatorBase extends HdrHistogramEvaluatorBase {

    /**
     * Return ObjectInspectors indicating the UDAF's output type is a string.
     * 
     * @return ObjectInspector for the output of the UDAF.
     */
    protected ObjectInspector getOutputOIs() {
        // The output of FINAL and COMPLETE is a base64-encoded histogram
        ObjectInspector encodedHistogramOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        return encodedHistogramOI;
    }

    /**
     * Encode the histogram into a base64-encoded string.
     * 
     * @param histogram The histogram to encode.
     * @return A base64-encoded Hive Text object representing the histogram.
     */
    protected Object getResult(AbstractHistogram histogram) {

        ByteBuffer bbuf = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
        histogram.encodeIntoCompressedByteBuffer(bbuf);
        bbuf.flip();
        ByteBuffer base64 = java.util.Base64.getEncoder().encode(bbuf);

        Text encodedHistogram = new Text();
        
        encodedHistogram.set(new String(base64.array()));
        return encodedHistogram;
    };

}