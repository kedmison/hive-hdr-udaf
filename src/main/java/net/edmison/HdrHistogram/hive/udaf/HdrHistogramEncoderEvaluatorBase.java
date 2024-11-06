package net.edmison.HdrHistogram.hive.udaf;

import java.nio.ByteBuffer;
import org.HdrHistogram.AbstractHistogram;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.io.Text;

public abstract class HdrHistogramEncoderEvaluatorBase extends HdrHistogramEvaluatorBase {

    protected ObjectInspector getOutputOIs() {
        // The output of FINAL and COMPLETE is a base64-encoded histogram
        ObjectInspector encodedHistogramOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        return encodedHistogramOI;
    }

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