package com.deerlili.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author deerlili
 * @date 2022/4/8
 * @des 无界流
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从参数中提取端口号和主机
        // 配置:在当前代码Run-Eeit Configurations-Programs args 添加--host hadoop100 --port 7777
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");

        // 2. 读取文本流
        // 在linux上发送数据（命令）：yum install -y nc 执行：nc -lk 7777
        DataStreamSource<String> lineDataStream = env.socketTextStream(host, port);
        // 3. 装换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneTuple = lineDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word,1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordOneGroup = wordOneTuple.keyBy(data -> data.f0);
        // 5. 聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordOneGroup.sum(1);
        // 6. 输出
        sum.print();
        // 7. 执行
        env.execute();
    }
}
