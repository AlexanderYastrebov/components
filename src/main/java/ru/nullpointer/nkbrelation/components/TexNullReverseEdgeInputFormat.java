package ru.nullpointer.nkbrelation.components;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author Alexander Yastrebov
 */
public class TexNullReverseEdgeInputFormat extends TextEdgeInputFormat<Text, NullWritable> {

    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public EdgeReader<Text, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        EdgeReader<Text, NullWritable> edgeReader = new TextNullEdgeReader();;
        edgeReader.setConf(getConf());
        return new ReverseEdgeDuplicator<Text, NullWritable>(edgeReader);
    }

    private class TextNullEdgeReader extends TextEdgeReaderFromEachLineProcessed<TextPair> {

        @Override
        protected TextPair preprocessLine(Text line) throws IOException {
            String[] tokens = SEPARATOR.split(line.toString());
            return new TextPair(new Text(tokens[0]), new Text(tokens[1]));
        }

        @Override
        protected Text getSourceVertexId(TextPair endpoints)
                throws IOException {
            return new Text(endpoints.getFirst());
        }

        @Override
        protected Text getTargetVertexId(TextPair endpoints)
                throws IOException {
            return new Text(endpoints.getSecond());
        }

        @Override
        protected NullWritable getValue(TextPair endpoints) throws IOException {
            return NullWritable.get();
        }
    }
}
