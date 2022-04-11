//package com.deerlili.wc;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
///**
// * @author deerlili
// * @date 2022/4/9
// * @des
// */
//public class DataStreamAndTable {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        DataStream<String> dataStream = env.fromElements("Alice", "Bob", "John");
//        Table inputTable = tableEnv.fromDataStream(dataStream);
//        tableEnv.createTemporaryView("InputTable",dataStream);
//        Table resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable");
//        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);
//        resultStream.print();
//        env.execute();
//    }
//}
