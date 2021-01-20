package com.bgg.flink.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink08_TransForm_Filter_RichFilter <br/>
 * Description: <br/>
 * date: 2021/1/19 18:45<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink09_TransForm_Filter_RichFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> textFile = env.readTextFile("input/sensor.txt");

        textFile.filter(new FilterFunc()).print();
        env.execute();
    }

     public static  class FilterFunc implements  FilterFunction<String>{

         @Override
         public boolean filter(String s) throws Exception {
             String[] split = s.split(",");
             return Integer.parseInt(split[2] ) > 30;
         }
     }
}
