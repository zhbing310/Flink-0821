package com.bgg.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * ClassName: Flink01_WordCount <br/>
 * Description: <br/>
 * date: 2021/1/18 10:04<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取文件
        DataSource<String> input = env.readTextFile("input");
        //压平数据
        FlatMapOperator<String, String> wordDs = input.flatMap(new FlatMapFunc());

        //转换为元组

        MapOperator<String, Tuple2<String, Integer>>  wordToOneDs =  wordDs.map((MapFunction<String, Tuple2<String, Integer>>) value -> {
            return new Tuple2<>(value, 1);
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        //分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOneDs.groupBy(0);
        //聚合
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);
        result.print();
    }
    public static  class FlatMapFunc implements FlatMapFunction<String,String>{

        @Override
        public void flatMap(String s, Collector<String> out) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
               out.collect(word); 
            }
        }
    }
}

