package net.edmison.HdrHistogram.hive.udaf;

import org.HdrHistogram.Histogram;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/**
 * HdrEncodedHistogramEvaluator is a user-defined aggregation function (UDAF) for Apache Hive
 * that aggregates HDR Histograms.
 *
 * This class is responsible for parsing string values containing base64-encoded 
 * histograms and aggregating them into a histogram. 
 * 
 * This class leverages the getResult method of HdrHistogramEvaluatorBase to return 
 * the aggregated HDR Histogram as an array of structs in the table/CSV format 
 * produced by HDR Histogram tools.
 */ 

public class HdrEncodedHistogramEvaluator extends HdrHistogramEvaluatorBase {

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