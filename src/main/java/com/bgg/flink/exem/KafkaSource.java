package com.bgg.flink.exem;

import com.bgg.flink.day02.Flink05_Source_Custom;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * ClassName: KafkaSource <br/>
 * Description: <br/>
 * date: 2021/1/20 8:46<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"flink0821");
        DataStreamSource<String> test = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));

        test.print();
        env.execute();

    }
}
