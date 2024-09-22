package com.hummingbird.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 * 带环境的 配置读取工具类
 */
public class ParameterToolEnvironmentUtils {
    public static final String defaultConfigFileName = "application.properties";
    public static final String environmentFileNameTemplate = "application-%s.properties";
    public static final String ENV_ACTIVE = "env.active";


    public static ParameterTool createParameterTool(String[] args) throws IOException, IOException {

        //系统环境参数 -Dyy=xx
        ParameterTool systemProperties = ParameterTool.fromSystemProperties();

        //运行参数 main参数  flink自己数据需要 - 或者 -- 开头做key 例如 -name tom or --name tom
        ParameterTool fromArgs = ParameterTool.fromArgs(args);

        //默认配置文件
        ParameterTool defaultPropertiesFile = ParameterTool
                .fromPropertiesFile(Thread.currentThread().getContextClassLoader().getResourceAsStream(defaultConfigFileName));

        //按照优先级获取有效环境值
        String envActiveValue = getEnvActiveValue(systemProperties,fromArgs,defaultPropertiesFile);
        String currentEnvFileName = String.format(environmentFileNameTemplate, envActiveValue);

        //读取合并环境参数
        ParameterTool currentEnvPropertiesFile = ParameterTool.fromPropertiesFile(Thread.currentThread().getContextClassLoader().getResourceAsStream(currentEnvFileName));
        ParameterTool parameterTool = currentEnvPropertiesFile
                .mergeWith(fromArgs)
                .mergeWith(defaultPropertiesFile)
                .mergeWith(systemProperties);

        System.out.println("=====================================应用活动配置======================================================");
        currentEnvPropertiesFile.getProperties().forEach((k, v) -> System.out.println(k + ":" + v));

        System.out.println("=====================================应用启动参数======================================================");
        fromArgs.getProperties().forEach((k, v) -> System.out.println(k + ":" + v));


        System.out.println("=====================================应用默认配置======================================================");
        defaultPropertiesFile.getProperties().forEach((k, v) -> System.out.println(k + ":" + v));


        System.out.println("=====================================系统配置======================================================");
        systemProperties.getProperties().forEach((k, v) -> System.out.println(k + ":" + v));

        System.out.println("=====================================合并后配置======================================================");
        parameterTool.getProperties().forEach((k, v) -> System.out.println(k + ":" + v));
        System.out.println("=================================================================================================");



        return parameterTool;
    }

    /**
     * 按照优先级获取有效环境值
     *
     * @return
     */
    public static String getEnvActiveValue(ParameterTool systemProperties, ParameterTool fromArgs, ParameterTool defaultPropertiesFile) {
        //选择参数环境
        String env;
        if (fromArgs.has(ENV_ACTIVE)) {
            env = fromArgs.get(ENV_ACTIVE);
        } else if (defaultPropertiesFile.has(ENV_ACTIVE)) {
            env = defaultPropertiesFile.get(ENV_ACTIVE);
        } else if (systemProperties.has(ENV_ACTIVE)) {
            env = systemProperties.get(ENV_ACTIVE);
        } else {
            //如果没有配置抛出异常
            throw new IllegalArgumentException(String.format("%s does not exist！ Please set up the environment. for example： application.properties Add configuration env.active = dev", ENV_ACTIVE));
        }
        return env;
    }
}