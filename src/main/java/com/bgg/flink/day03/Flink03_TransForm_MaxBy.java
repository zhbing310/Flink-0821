package com.bgg.flink.day03;

import com.bgg.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: Flink02_TransForm_Max <br/>
 * Description: <br/>
 * date: 2021/1/20 11:09<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink03_TransForm_MaxBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketText = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> map = socketText.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.valueOf(split[2]));
            }
        });

        KeyedStream<WaterSensor, String> keyBy = map.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        SingleOutputStreamOperator<WaterSensor> max = keyBy.maxBy("vc",true);
        max.print();
        env.execute();
    }
}
