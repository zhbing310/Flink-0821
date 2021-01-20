package com.bgg.flink.day02;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * ClassName: Flink09_TransForm_Connect_RichConnect <br/>
 * Description: <br/>
 * date: 2021/1/19 18:53<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink10_TransForm_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> StringDS = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 8888);

       // SingleOutputStreamOperator<Integer> DS = streamSource.map(s -> s.length());
        SingleOutputStreamOperator<Integer> DS = streamSource.map(String::length);

        ConnectedStreams<String, Integer> connect = StringDS.connect(DS);
        SingleOutputStreamOperator<Object> result = connect.map(new CoMapFunction<String, Integer, Object>() {


            @Override
            public Object map1(String s) throws Exception {
                return s;
            }

            @Override
            public Object map2(Integer value) throws Exception {
                return value;
            }
        });

        result.print();

        env.execute();
    }
}
