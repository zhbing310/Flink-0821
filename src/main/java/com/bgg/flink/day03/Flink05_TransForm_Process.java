package com.bgg.flink.day03;

import com.bgg.flink.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName: Flink05_TransForm_Process <br/>
 * Description: <br/>
 * date: 2021/1/20 18:17<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink05_TransForm_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketText = env.socketTextStream("hadoop102", 9999);
        //实现压平
        SingleOutputStreamOperator<String> flatMap = socketText.process(new ProcessFlatMapFunc());

        //实现map
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToDs = flatMap.process(new ProcessMapFunc());

        //分组
        KeyedStream<Tuple2<String, Integer>, String> keyBy = wordToDs.keyBy(data -> data.f0);

        //求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyBy.sum(1);

        result.print();

        env.execute();


    }

    public static class ProcessFlatMapFunc extends ProcessFunction<String, String> {
        @Override
        public void processElement(String s, Context context, Collector<String> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(word);
            }
        }
    }
    public static class ProcessMapFunc extends  ProcessFunction<String,Tuple2<String,Integer>>{

        @Override
        public void processElement(String s, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
           collector.collect(new Tuple2<>(s,1));
        }
    }
}

