package ru.nullpointer.nkbrelation.components;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Alexander Yastrebov
 */
public class JsonEdgeInputFormat extends TextEdgeInputFormat<Text, NullWritable> {

    private Logger logger = LoggerFactory.getLogger(JsonEdgeInputFormat.class);
    //

    @Override
    public EdgeReader<Text, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        JsonEdgeReader reader = new JsonEdgeReader();
        reader.setConf(getConf());
        return new ReverseEdgeDuplicator<Text, NullWritable>(reader);
    }

    private class JsonEdgeReader extends TextEdgeReader {

        private JSONObject next;

        @Override
        public boolean nextEdge() throws IOException, InterruptedException {
            try {
                return getNextObject();
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }

        @Override
        public Text getCurrentSourceId() throws IOException, InterruptedException {
            return getField("_srcId");
        }

        @Override
        public Edge<Text, NullWritable> getCurrentEdge() throws IOException, InterruptedException {
            Text dstId = getField("_dstId");
            return EdgeFactory.create(dstId);
        }

        private boolean getNextObject() throws Exception {
            next = null;
            RecordReader<LongWritable, Text> recordReader = getRecordReader();
            while (recordReader.nextKeyValue()) {
                String line = recordReader.getCurrentValue().toString();
                if (!StringUtils.isBlank(line)) {
                    next = new JSONObject(line);
                    break;
                }
            }
            return (next != null);
        }

        private Text getField(String key) throws IOException {
            try {
                return new Text(next.getString(key));
            } catch (JSONException ex) {
                throw new IOException(ex);
            }
        }
    }
}
