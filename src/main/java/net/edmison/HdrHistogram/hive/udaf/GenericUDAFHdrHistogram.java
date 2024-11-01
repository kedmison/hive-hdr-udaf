package net.edmison.HdrHistogram.hive.udaf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.AbstractHistogram.Percentiles;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * be inspired by GenericUDAFHistogramNumeric.java
 *
 */
@SuppressWarnings("deprecation")
@Description(name = "hdr_histogram", value = "_FUNC_(x, precision) - Returns a HDR Histogram JSON of a set of numbers", extended = "Example: \n"
    + "SELECT hdr_histogram(val, 3) from src;\n"
    // TODO insert example output representation, could do x/y like
    // HistogramNumeric.
    + "This creates a HDR histogram for the specified set, with the specified result precision")
public class GenericUDAFHdrHistogram extends AbstractGenericUDAFResolver {
  static final Logger LOG = LoggerFactory.getLogger(GenericUDAFHdrHistogram.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "please specify two arguments.");
    }

    // validate the second parameter, which is the histogram precision
    if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1,
          "Only primitive type arguments are accepted but "
              + parameters[1].getTypeName() + " was passed as parameter 2.");
    }

    if (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
      throw new UDFArgumentTypeException(1,
          "Only an integer argument is accepted as parameter 2, but "
              + parameters[1].getTypeName() + " was passed instead.");
    }

    // validate the first argument which is the column/data to be measured in the
    // histogram.
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "This function cannot process arguments of type "
              + parameters[0].getTypeName() + " .");
    }

    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
      case BYTE: 
      case SHORT:
      case INT: 
      case LONG:
        return new HdrHistogramEvaluator();
      case STRING:
      case VARCHAR:
      case CHAR:
        return new HdrEncodedHistogramEvaluator();
      case FLOAT:
      case DOUBLE:
      case TIMESTAMP:
      case DECIMAL:
      case BOOLEAN:
      case DATE:
      default:
        throw new UDFArgumentTypeException(0,
            "Only discrete numeric arguments are accepted but "
                + parameters[0].getTypeName() + " is passed.");
    }
  }

  public static class HdrHistogramEvaluator extends HdrHistogramEvaluatorBase {

    protected void recordValue(HdrHistogramAggregationBuffer myagg, Object vObject) {
      long longValue = PrimitiveObjectInspectorUtils.getLong(vObject, valueOI);
      if (longValue > 0) {
        myagg.histogram.recordValue(longValue);
      } else {
        if (!warned) {
          LOG.warn("HDR Histogram cannot process zero or negative values; Ignoring value <= 0: " + longValue);
          warned = true;
        }
      }
    }

  }

  public static class HdrEncodedHistogramEvaluator extends HdrHistogramEvaluatorBase {

    protected void recordValue(HdrHistogramAggregationBuffer myagg, Object vObject) throws HiveException {
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

  public abstract static class HdrHistogramEvaluatorBase extends GenericUDAFEvaluator {
    // For input to PARTIAL1 and COMPLETE: ObjectInspectors for input data
    protected transient PrimitiveObjectInspector valueOI;
    protected transient PrimitiveObjectInspector precisionOI;

    // For intermediate processing:
    // for input to PARTIAL2 and FINAL:
    // for output of PARTIAL1 and PARTIAL2:
    // this stores the compressed bytes of a serialized HDR Histogram
    // no attempt is made to directly and hadoop-natively serialize the internals
    // of the HDR histogram.
    protected transient WritableBinaryObjectInspector partialOI;

    // For output of FINAL and COMPLETE
    // this will be an array of tuples of
    // value/percentile/totalcount/PercentileLogScale values
    private transient ListObjectInspector datapointsOI;

    // Helps attenuate warnings issued for values <=0 that HDR Histogram
    // cannot process.
    protected boolean warned = false;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);

      partialOI = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

      // The output of FINAL and COMPLETE is a
      // list of DoubleWritable structs that represent the final histogram as
      // value, percentile, totalcount, pctlogscale values similar to what is
      // produced in text output of HDR Histograms.
      // one can plot on a scatter-plot by using value as the 'y' axis and
      // pctlogscale as the 'x' axis, and using a logarithmic axis for the 'x' axis.
      // this will replicate the HDR Histogram charts.
      ArrayList<String> fieldNames = new ArrayList<String>();
      ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
      fieldNames.add("value");
      fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
      fieldNames.add("percentile");
      fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
      fieldNames.add("totalcount");
      fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
      fieldNames.add("pctlogscale");
      fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

      datapointsOI = ObjectInspectorFactory.getStandardListObjectInspector(
          ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs));

      // init input parameters
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        assert (parameters.length == 2);
        valueOI = (PrimitiveObjectInspector) parameters[0];
        precisionOI = (PrimitiveObjectInspector) parameters[1];
      }

      switch (m) {
        case PARTIAL1:
        case PARTIAL2:
          return partialOI;
        case FINAL:
        case COMPLETE:
          return datapointsOI;
        default:
          throw new IllegalArgumentException("Unknown mode: " + m);
      }

    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      HdrHistogramAggregationBuffer result = new HdrHistogramAggregationBuffer();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      HdrHistogramAggregationBuffer myagg = (HdrHistogramAggregationBuffer) agg;
      myagg.histogram = null;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      HdrHistogramAggregationBuffer myagg = (HdrHistogramAggregationBuffer) agg;
      if (myagg.histogram == null) {
        // initialize the histogram using the parameters context, 
        // to set the precision
        int precision = PrimitiveObjectInspectorUtils.getInt(parameters[1], precisionOI);
        myagg.histogram = new Histogram(precision);
      }      

      // Process the current data point
      recordValue(myagg, parameters[0]);
    }

    protected abstract void recordValue(HdrHistogramAggregationBuffer myagg, Object vObject) throws HiveException;




    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      HdrHistogramAggregationBuffer myagg = (HdrHistogramAggregationBuffer) agg;
      BytesWritable bw = null;
      if (myagg.histogram != null) {
        ByteBuffer buf = ByteBuffer.allocate(myagg.histogram.getNeededByteBufferCapacity());
        myagg.histogram.encodeIntoByteBuffer(buf);
        bw = new BytesWritable(buf.array(), buf.position());
      }
      return bw;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial == null) {
        return;
      }
      HdrHistogramAggregationBuffer myagg = (HdrHistogramAggregationBuffer) agg;
      BytesWritable bw = partialOI.getPrimitiveWritableObject(partial);
      ByteBuffer buf = ByteBuffer.wrap(bw.getBytes());
      Histogram other = Histogram.decodeFromByteBuffer(buf, 0);
      if (myagg.histogram == null) {
        myagg.histogram = other;
      } else {
        myagg.histogram.add(other);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      HdrHistogramAggregationBuffer myagg = (HdrHistogramAggregationBuffer) agg;
      if (myagg.histogram == null || myagg.histogram.getTotalCount() == 0) {
        return null;
      }

      AbstractHistogram hgram = myagg.histogram;

      ArrayList<DoubleWritable[]> result = getDataPoints(hgram);
      return result;
    }

    protected static ArrayList<DoubleWritable[]> getDataPoints(AbstractHistogram hgram) {
      Percentiles percentiles = hgram.percentiles(5);
      Iterator<HistogramIterationValue> iterator = percentiles.iterator();
      ArrayList<DoubleWritable[]> result = new ArrayList<DoubleWritable[]>();
      while (iterator.hasNext()) {
        HistogramIterationValue iterationValue = iterator.next();
        DoubleWritable[] datapoint = new DoubleWritable[4];
        if (iterationValue.getPercentileLevelIteratedTo() != 100.0D) {
          datapoint[0] = new DoubleWritable(iterationValue.getValueIteratedTo() / 1.0D);
          datapoint[1] = new DoubleWritable(iterationValue.getPercentileLevelIteratedTo() / 100.0D);
          datapoint[2] = new DoubleWritable(iterationValue.getTotalCountToThisValue());
          datapoint[3] = new DoubleWritable(1 / (1.0D - (iterationValue.getPercentileLevelIteratedTo() / 100.0D)));
          result.add(datapoint);
        }
      }
      return result;
    };

    @AggregationType(estimable = true)
    protected static class HdrHistogramAggregationBuffer extends AbstractAggregationBuffer {
      protected Histogram histogram;

      @Override
      public int estimate() {
        int estimatedSize = histogram == null ? 0 : histogram.getNeededByteBufferCapacity();
        return estimatedSize;
      }
    }

  }

}
