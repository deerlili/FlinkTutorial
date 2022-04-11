package com.deerlili.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author deerlili
 * @date 2022/4/8
 * @des 有界数据流
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");
        // 3. 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordOneTupleGroup = wordOneTuple.keyBy(data -> data.f0);
        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordOneTupleGroup.sum(1);
        // 6. 打印输出
        sum.print();
        // 7. 启动执行
        env.execute();
    }
}
