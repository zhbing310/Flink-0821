package com.bgg.flink.day02;

import com.bgg.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink01_TransForm_Map_Anonymous <br/>
 * Description: <br/>
 * date: 2021/1/19 18:13<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink06_TransForm_Map_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> textFile = env.readTextFile("input/sensor.txt");
        //第一种
      /*  SingleOutputStreamOperator<WaterSensor> map = textFile.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");

                return new WaterSensor(split[1], Long.parseLong(split[1]), Integer.valueOf(split[2]));
            }
        });*/
      //第二种方式
        //SingleOutputStreamOperator<WaterSensor> map = textFile.map(new MapFunct());
       // 第三种方式
        SingleOutputStreamOperator<WaterSensor> map;
        map = textFile.map(new MapFunct());
        map.print();
        env.execute();
    }

    public static class  MapFunct implements  MapFunction<String,WaterSensor>{

        @Override
        public WaterSensor map(String s) throws Exception {
            String[] split = s.split(",");

            return new WaterSensor(split[1], Long.parseLong(split[1]), Integer.valueOf(split[2]));
        }
    }
}
