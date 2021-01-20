package com.bgg.flink.day03;

import com.bgg.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * ClassName: Flink11_Sink_JDBC <br/>
 * Description: <br/>
 * date: 2021/1/20 21:01<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink11_Sink_JDBC {
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

        //将数据写入mysql
        WordToMap.addSink(JdbcSink.sink(
                "INSERT INTO `sensor` VALUES(?,?,?) ON DUPLICATE KEY UPDATE `ts`=?,`vc`=?"
        ,new JdbcStatementBuilder<WaterSensor>(){
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1,waterSensor.getId());
                        preparedStatement.setLong(2,waterSensor.getTs());
                        preparedStatement.setInt(3,waterSensor.getVc());
                        preparedStatement.setLong(4,waterSensor.getTs());
                        preparedStatement.setInt(5,waterSensor.getVc());
                    }
                },
                JdbcExecutionOptions.builder().withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                 .build()
        ));
         env.execute();
    }
}
