package com.bgg.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName: Flink02_WordCount_Bounded <br/>
 * Description: <br/>
 * date: 2021/1/18 11:33<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       //读取文件
        DataStreamSource<String> input = env.readTextFile("input");

        //拍平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneWordDs = input.flatMap(new LineToTupleFunc());
        //分组
        KeyedStream<Tuple2<String, Integer>, String> keyBy = wordToOneWordDs.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyBy.sum(1);
        result.print();

        env.execute();
    }

    public  static class LineToTupleFunc implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
             collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
