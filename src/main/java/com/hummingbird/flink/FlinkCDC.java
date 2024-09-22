package com.hummingbird.flink;

import com.hummingbird.flink.streaming.FlinkMysql2Mysql;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        FlinkMysql2Mysql flinkCDCMigrationJob = new FlinkMysql2Mysql();
        flinkCDCMigrationJob.execute(args);
    }

}
