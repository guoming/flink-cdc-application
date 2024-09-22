package com.hummingbird.flink.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hummingbird.flink.utils.ParameterToolEnvironmentUtils;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.util.stream.Collectors;


public class FlinkMysql2Mysql implements  Serializable {

    String JobName;
    String targetTablePrimaryKey;
    String targetUrl;
    String targetUser;
    String targetPassword ;


    String sourceDBHostName;
    Integer sourceDbPort;
    String sourceDbUserName;
    String sourceDbPassword;

    String sourceDbList;
    String sourceTableList;
    String sourceTablePrimaryKey;

    private void loadProperties(Properties properties) {


        JobName = properties.getProperty("app.name");
        sourceDbList = properties.getProperty("app.source.db-list");
        sourceTableList = properties.getProperty("app.source.table-list");
        sourceTablePrimaryKey = properties.getProperty("app.source.table.primary-key");

        sourceDBHostName = properties.getProperty("app.source.connection.host");
        sourceDbPort = Integer.parseInt(properties.getProperty("app.source.connection.port"));
        sourceDbUserName = properties.getProperty("app.source.connection.username");
        sourceDbPassword = properties.getProperty("app.source.connection.password");


        targetUrl = properties.getProperty("app.sink.connection.url");
        targetUser = properties.getProperty("app.sink.connection.username");
        targetPassword = properties.getProperty("app.sink.connection.password");
        targetTablePrimaryKey = properties.getProperty("app.sink.table.primary-key");

    }

    public void execute(String[] args) throws Exception {


        ParameterTool parameterTool = ParameterToolEnvironmentUtils.createParameterTool(args);
        loadProperties(parameterTool.getProperties());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      //  env.getConfig().setGlobalJobParameters(parameterTool);

        // 作业取消后仍然保存Checkpoint
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend(
                "file:///opt/flink/checkpoints",
                false));


        env.getCheckpointConfig().setCheckpointStorage("file:///opt/flink/checkpoints/1dcbf2d8036123f6d0886198bd43b63f");

        // 或者设置从savepoint恢复

        // 从 MySQL CDC 读取数据流
        DataStreamSource<String> source = env.addSource(MySqlSource.<String>builder()
                .hostname(sourceDBHostName)
                .port(sourceDbPort)
                .username(sourceDbUserName)
                .password(sourceDbPassword)
                .databaseList(sourceDbList) // 数据库名
                .tableList(sourceTableList) // 要监听的表
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema(false))
                .build());

        //拆分流
        SingleOutputStreamOperator<Tuple2<Integer, String>> splitStream = source.map(new SplitTableSink());

        splitStream.addSink(new MySQLSink());

        env.execute(JobName);

    }

    public class SplitTableSink implements MapFunction<String, Tuple2<Integer, String>>, Serializable {
        @Override
        public Tuple2<Integer, String> map(String json) throws Exception {

            JSONObject rowData = JSON.parseObject(json);

            System.out.println(json);

            //操作类型
            String operator = rowData.getString("op");

            //删除操作, 撤回流处理
            if(operator.equals("d"))
            {
                JSONObject beforeObject = rowData.getJSONObject("before");

                //订a单id(约定所有表都有这个字段)
                long orderId = beforeObject.getLong(sourceTablePrimaryKey);

                //计算分表的名称
                int tableIndex = (int) (orderId % 16);

                return new Tuple2<>(tableIndex, json);
            }
            //新增或者更新
            else
            {

                JSONObject afterObject = rowData.getJSONObject("after");

                //订单id(约定所有表都有这个字段)
                long orderId = afterObject.getLong(sourceTablePrimaryKey);

                //计算分表的名称
                int tableIndex = (int) (orderId % 16);

                return new Tuple2<>(tableIndex, json);
            }
        }
    }

    // 自定义 MySQL Sink，按表插入数据
    public class MySQLSink extends RichSinkFunction<Tuple2<Integer, String>> {
        @Override
        public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {

            int tableIndex = value.f0;

            JSONObject rowData = JSON.parseObject(value.f1);
            //操作类型
            String operator = rowData.getString("op");

            JSONObject sourceObject = rowData.getJSONObject("source");

            //目标表名称
            String targetTableName = String.format("%s_%s", sourceObject.getString("table"), tableIndex);

            Class.forName("com.mysql.jdbc.Driver");
            Connection targetConn = DriverManager.getConnection(targetUrl, targetUser, targetPassword);

            if(operator.equals("d")) {

                JSONObject beforeRecord = rowData.getJSONObject("before");

                String deleteSql = String.format("delete from  %s WHERE %s=?", targetTableName,targetTablePrimaryKey);
                PreparedStatement preparedStatement = targetConn.prepareStatement(deleteSql);
                preparedStatement.setObject(1, beforeRecord.get(targetTablePrimaryKey));
                preparedStatement.executeUpdate();
            }
            else if(operator.equals("u"))
            {
                JSONObject beforeRecord = rowData.getJSONObject("before");
                JSONObject afterRecord = rowData.getJSONObject("after");

                //自动生成更新语句
                String updateSql = String.format("UPDATE %s SET %s WHERE %s=?",
                        targetTableName,
                        afterRecord.keySet().stream().filter(key -> !key.equals(targetTablePrimaryKey)).map(key -> key + "=?").collect(Collectors.joining(", ")),
                        targetTablePrimaryKey);

                PreparedStatement preparedStatement = targetConn.prepareStatement(updateSql);

                int index = 1;
                for (String key : afterRecord.keySet()) {

                    if (key.equals(targetTablePrimaryKey)) {
                        continue;
                    }

                    preparedStatement.setObject(index, afterRecord.get(key));
                    index++;
                }

                preparedStatement.setObject(index, beforeRecord.get(targetTablePrimaryKey));
                preparedStatement.executeUpdate();

            }
            else
            {
                JSONObject afterRecord = rowData.getJSONObject("after");

                //自动生成插入语句
                String insertSql = String.format("INSERT IGNORE INTO %s(%s) VALUES (%s)",
                        targetTableName,
                        afterRecord.keySet().stream().map(key -> key).collect(Collectors.joining(", ")),
                        afterRecord.keySet().stream().map(key -> "?").collect(Collectors.joining(", ")));

                PreparedStatement preparedStatement = targetConn.prepareStatement(insertSql);
                int index = 1;
                for (String key : afterRecord.keySet()) {
                    preparedStatement.setObject(index, afterRecord.get(key));
                    index++;
                }
                preparedStatement.executeUpdate();
            }

        }
    }


}
