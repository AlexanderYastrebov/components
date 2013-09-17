package ru.nullpointer.nkbrelation.components;

import org.apache.giraph.combiner.Combiner;
import org.apache.hadoop.io.Text;


/**
 *
 * @author Alexander Yastrebov
 */
public class MinTextCombiner extends Combiner<Text, Text> {

    @Override
    public void combine(Text vertexIndex, Text originalMessage, Text messageToCombine) {
        if (originalMessage.getBytes().length == 0 || originalMessage.compareTo(messageToCombine) > 0) {
            originalMessage.set(messageToCombine.getBytes());
        }
    }

    @Override
    public Text createInitialMessage() {
        return new Text();
    }
}
