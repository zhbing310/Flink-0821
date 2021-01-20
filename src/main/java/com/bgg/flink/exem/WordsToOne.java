package com.bgg.flink.exem;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;


/**
 * ClassName: WordsToOne <br/>
 * Description: <br/>
 * date: 2021/1/20 8:56<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class WordsToOne {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // env.setParallelism(1);

        //DataStreamSource<String> textFile = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> textFile = env.readTextFile("input/word.txt");
        SingleOutputStreamOperator<String> flatMap = textFile.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {

                String[] split = s.split(" ");
                for (String s1 : split) {
                    collector.collect(s1);
                }

            }
        });

        
        SingleOutputStreamOperator<String> filter = flatMap.keyBy(x->x).filter(new FilterFunction<String>() {
            HashSet<String> set = new HashSet<>();
            @Override
            public boolean filter(String o) throws Exception {
                    if(set.contains(o)){
                       return  false;
                    }else{
                        set.add(o);
                        return  true;
                    }

            }
            //filter.print();

        });
        filter.print();
        env.execute();
    }
}