package com.bgg.flink.day02;

import com.bgg.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * ClassName: Flink05_Source_Custom <br/>
 * Description: <br/>
 * date: 2021/1/19 17:15<br/>
 *
 * @author BGG<br />
 * @since JDK 1.8
 */
public class Flink05_Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new Mysourc("hadoop102",9999)).print();

        env.execute();

    }

    public static  class Mysourc implements SourceFunction<WaterSensor>{
        private  String host;
        private  int port;
        private boolean running=true;
        Socket socket = null;
        BufferedReader reader = null;
        public Mysourc() {
        }

        public Mysourc(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
            socket=new Socket(host,port);
            reader=new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String line=null;
            while (running&&(line=reader.readLine())!=null){
                String[] split = line.split(",");
                sourceContext.collect(new WaterSensor(split[0],Long.parseLong(split[1]),Integer.valueOf(split[2])));
            }
        }

        @Override
        public void cancel() {
         running=false;
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
