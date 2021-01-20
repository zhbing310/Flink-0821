package com.bgg.flink.day03;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink_TransForm_Union <br/>
 * Description: <br/>
 * date: 2021/1/20 11:01<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink01_TransForm_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> socketTextStream2 = env.socketTextStream("hadoop102", 8888);

        DataStream<String> union = socketTextStream1.union(socketTextStream2);
        union.print();
        env.execute();
    }
}
