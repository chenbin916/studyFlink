package com.chenbin.study.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WorldCount {


    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> textStream = env.readTextFile("F:\\gitWorkSpace\\studyFlink_new\\studyFlink\\src\\main\\resources\\word.txt");

        FlatMapOperator<String, Tuple2<String, Integer>> wordStream = textStream.flatMap(new MyFlatMapFunction());
        UnsortedGrouping<Tuple2<String, Integer>> resultStream1=wordStream.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> resultStream2=resultStream1.sum(1);
        resultStream2.print("out");

        // execute program
        env.execute();
    }

   public  static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>>{
        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = in.split(" ");
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<String,Integer>(word, 1));
                }
            }
        }
    }

}



