package com.bgg.flink.day03;

import com.bgg.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * ClassName: Flink10_Sink_MySink <br/>
 * Description: <br/>
 * date: 2021/1/20 20:29<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink10_Sink_MySink {
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

        //将数据写入Mysql
         WordToMap.addSink(new MySink());

         env.execute();
    }

    public static  class MySink extends RichSinkFunction<WaterSensor>{
        //声明连接
        private Connection connection;
        private PreparedStatement preparedStatement;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection= DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false","root","123456");

          //preparedStatement=  connection.prepareStatement("INSERT INTO sensor VALUES (?,?,?) ON DUPLICATE KEY UPDATE 'ts'=?,'vc'=?");
            preparedStatement = connection.prepareStatement("INSERT INTO `sensor` VALUES(?,?,?) ON DUPLICATE KEY UPDATE `ts`=?,`vc`=?");

        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            preparedStatement.setString(1,value.getId());
            preparedStatement.setLong(2,value.getTs());
            preparedStatement.setInt(3,value.getVc());
            preparedStatement.setLong(4,value.getTs());
            preparedStatement.setInt(5,value.getVc());
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
