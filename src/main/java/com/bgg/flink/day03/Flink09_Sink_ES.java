package com.bgg.flink.day03;

import com.bgg.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * ClassName: Flink08_Sink_Redis <br/>
 * Description: <br/>
 * date: 2021/1/20 19:29<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink09_Sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> WordToMap = env.socketTextStream("hadoop102", 9999).map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //将数据写入Es
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<WaterSensor>(httpHosts, new MyEsSinkFunc());
        //批量提交参数
        waterSensorBuilder.setBulkFlushMaxActions(1);
        ElasticsearchSink<WaterSensor> elasticsearchSink = waterSensorBuilder.build();

        WordToMap.addSink(elasticsearchSink);
        env.execute();
    }
   public static class MyEsSinkFunc implements ElasticsearchSinkFunction<WaterSensor>{

       @Override
       public void process(WaterSensor waterSensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
           HashMap<String, String> hashMap = new HashMap<>();
           hashMap.put("ts",waterSensor.getTs().toString());
           hashMap.put("vc",waterSensor.getVc().toString());


           //创建Index请求
           IndexRequest indexRequest = Requests.indexRequest()
                   .index("sersor1")
                   .type("_doc")
                   .id(waterSensor.getId())
                   //                    .id(element.getId())
                   .source(hashMap);
           //添加索引
           requestIndexer.add(indexRequest);
       }
   }
}
