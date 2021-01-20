package com.bgg.flink.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink02_Source_File <br/>
 * Description: <br/>
 * date: 2021/1/19 15:07<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/sensor.txt");
        stringDataStreamSource.print();
        env.execute();
    }
}
