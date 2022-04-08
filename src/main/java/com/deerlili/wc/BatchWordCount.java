package com.deerlili.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author deerlili
 * @date 2022/4/8
 * @apiNote 批处理，1.12.0之后弃用了，后面使用批流处理
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环节
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取文件数据
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");
        // 3. 每行分词，转换成二元组类型
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 3.1. 文本分词
            String[] words = line.split(" ");
            // 3.2. 单词转换成二元组输出
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 安装word进行分,0 第一个字段的索引
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
        // 5. 分组内进行聚合,1  第二个字段的索引
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);
        // 6. 打印输出
        sum.print();
    }
}
