package net.edmison.HdrHistogram.hive.udaf;

import org.HdrHistogram.Histogram;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class HdrEncodedHistogramEncoderEvaluator extends HdrHistogramEncoderEvaluatorBase {

    protected void recordValue(HdrHistogramEvaluatorBase.HdrHistogramAggregationBuffer myagg, Object vObject) throws HiveException {
      String value = PrimitiveObjectInspectorUtils.getString(vObject, valueOI);
      if (value != null) {
        try {
          Histogram other = Histogram.fromString(value);
          myagg.histogram.add(other);
        } catch (Exception e) {
          throw new HiveException(e);
        }
      }
    }

  }