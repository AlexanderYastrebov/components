#!/bin/sh

COMP_HOME=/home/alexander/projects/java/nkbcomponents/
JAR=$COMP_HOME/target/nkbcomponents-1.0-SNAPSHOT-jar-with-dependencies.jar

export HADOOP_CLASSPATH=$JAR:$LIBJARS

#ru.nullpointer.nkbrelation.components.TexNullReverseEdgeInputFormat

hadoop \
jar $JAR org.apache.giraph.GiraphRunner \
ru.nullpointer.nkbrelation.components.ConnectedComponentsComputation \
-ca ConnectedComponentsComputation.maxSize=10 \
-eif ru.nullpointer.nkbrelation.components.JsonEdgeInputFormat \
-eip /user/hdfs/input/giraph/relations.json \
-vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
-op /user/hdfs/output/giraph/components -w 1

