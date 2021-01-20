package com.bgg.flink.day02;

import com.bgg.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ClassName: Flink08_TransForm_FlatMap_RichFlatMap <br/>
 * Description: <br/>
 * date: 2021/1/19 18:33<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink08_TransForm_FlatMap_RichFlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> textFile = env.readTextFile("input/sensor.txt");
         textFile.flatMap(new RichFlatMapFuc()).print();
         env.execute();
    }
    public static class  RichFlatMapFuc extends RichFlatMapFunction<String,WaterSensor>{

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open ... 执行一次");
        }

        @Override
        public void flatMap(String s, Collector<WaterSensor> collector) throws Exception {
            String[] split = s.split(",");
            collector.collect(new WaterSensor(split[0],Long.parseLong(split[1]),Integer.valueOf(split[2])));
        }

        @Override
        public void close() throws Exception {
            System.out.println("close ... 执行一次");
        }
    }
}
