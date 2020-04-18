package com.hts.demo;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.beans.PropertyVetoException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
public class DemoApplication {

    /**
     * 配置信息
     */
    private static Map<String, String> configMap = new HashMap<>();

    /**
     * 需要监听的配置路径
     */
    private static final String CONFIG_PREFIX = "/CONFIG";

    /**
     * 数据源
     */
    private static ComboPooledDataSource dataSource;


    public static void main(String[] args) throws Exception {
        SpringApplication.run(DemoApplication.class, args);

        DemoApplication demo = new DemoApplication();
        demo.start();
    }


    private void start() throws Exception {
        ZkClient zkClient = new ZkClient("127.0.0.1:2181");
        System.out.println("会话被建立了");

        // 拿到config下的所有子节点，并且方到map中
        List<String> childNames = zkClient.getChildren(CONFIG_PREFIX);
        for (String childName : childNames) {
            String value = zkClient.readData(CONFIG_PREFIX + "/" + childName);
            configMap.put(childName, value);
        }

        // 读取节点信息放到map中
        setConfigToMap(zkClient);

        // 根据保持到map中的信息初始化数据源
        initDataSource();

        // 开启监听
        startListener(zkClient);
    }

    /**
     * 保持配置到map中
     *
     * @param zkClient
     */
    private void setConfigToMap(ZkClient zkClient) {
        List<String> childNames = zkClient.getChildren(CONFIG_PREFIX);
        for (String childName : childNames) {
            String value = zkClient.readData(CONFIG_PREFIX + "/" + childName);
            configMap.put(childName, value);
        }
    }

    /**
     * 初始化数据源信息
     *
     * @throws PropertyVetoException
     */
    private void initDataSource() throws PropertyVetoException {
        // 初始化datasource
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass(configMap.get("c3p0.driverClassName"));
        dataSource.setJdbcUrl(configMap.get("c3p0.dbJDBCUrl"));
        dataSource.setUser(configMap.get("c3p0.username"));
        dataSource.setPassword(configMap.get("c3p0.password"));

        this.dataSource = dataSource;
    }

    /**
     * 启动监听
     *
     * @param zkClient
     */
    private void startListener(ZkClient zkClient) {
        zkClient.subscribeDataChanges(CONFIG_PREFIX, new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {
                // 监听到配置发生改变，会触发
                setConfigToMap(zkClient);

                initDataSource();

                if (dataSource != null) {
                    dataSource.close();
                }

            }

            @Override
            public void handleDataDeleted(String s) throws Exception {

            }
        });
    }
}
