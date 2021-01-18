package com.bgg.flink.day01;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink03_WordCount_Unbounded <br/>
 * Description: <br/>
 * date: 2021/1/18 11:46<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink03_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //读取文件
        DataStreamSource<String> textStream = env.socketTextStream("hadoop102", 9999).setParallelism(2);

        //拍平转换为元组

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = textStream.flatMap(new Flink02_WordCount_Bounded.LineToTupleFunc()).setParallelism(3);

        //分组
        KeyedStream<Tuple2<String, Integer>, String> keyBy = wordToOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyBy.sum(1);
         result.print().setParallelism(6);
        env.execute();
    }
}
