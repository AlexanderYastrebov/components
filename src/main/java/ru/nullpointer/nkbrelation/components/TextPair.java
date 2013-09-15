package ru.nullpointer.nkbrelation.components;

import org.apache.hadoop.io.Text;

/**
 *
 * @author Alexander Yastrebov
 */
class TextPair {

    private Text first;
    private Text second;

    public TextPair(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }
}
