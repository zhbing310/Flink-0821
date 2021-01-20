package com.bgg.flink.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink03_Source_Socket <br/>
 * Description: <br/>
 * date: 2021/1/19 16:36<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink03_Source_Socket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        socketTextStream.print();

        env.execute();
    }
}
