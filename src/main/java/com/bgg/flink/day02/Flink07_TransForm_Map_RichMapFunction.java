package com.bgg.flink.day02;

import com.bgg.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink01_TransForm_Map_RichMapFunction <br/>
 * Description: <br/>
 * date: 2021/1/19 18:14<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink07_TransForm_Map_RichMapFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> textFile = env.readTextFile("input/sensor.txt");
        SingleOutputStreamOperator<WaterSensor> map = textFile.map(new MapFunct());

        map = textFile.map(new Flink06_TransForm_Map_Anonymous.MapFunct());
        map.print();
        env.execute();
    }

    public static class  MapFunct extends RichMapFunction<String, WaterSensor> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open ... 执行一次");
        }

        @Override
        public WaterSensor map(String s) throws Exception {
            String[] split = s.split(",");

            return new WaterSensor(split[1], Long.parseLong(split[1]), Integer.valueOf(split[2]));
        }

        @Override
        public void close() throws Exception {
            System.out.println("close ... 执行一次");
        }
    }
}
