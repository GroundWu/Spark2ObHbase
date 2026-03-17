package com.example.spark.obkv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * 构建 OBKV-HBase 连接所需的 Hadoop Configuration。
 * 可在代码中设置连接参数，或在 classpath 下提供 hbase-site.xml。
 */
public final class ObkvHBaseConfig {

    private ObkvHBaseConfig() {}

    /** 直连模式：从 obconfig 获取 RS 列表的 URL */
    public static final String HBASE_OCEANBASE_PARAM_URL = "hbase.oceanbase.paramURL";
    /** 格式 userName@tenantName#clusterName */
    public static final String HBASE_OCEANBASE_FULL_USER_NAME = "hbase.oceanbase.fullUserName";
    public static final String HBASE_OCEANBASE_PASSWORD = "hbase.oceanbase.password";
    public static final String HBASE_OCEANBASE_SYS_USER_NAME = "hbase.oceanbase.sysUserName";
    public static final String HBASE_OCEANBASE_SYS_PASSWORD = "hbase.oceanbase.sysPassword";

    /** 云上模式 */
    public static final String HBASE_OCEANBASE_ODP_ADDR = "hbase.oceanbase.odpAddr";
    public static final String HBASE_OCEANBASE_ODP_PORT = "hbase.oceanbase.odpPort";
    public static final String HBASE_OCEANBASE_ODP_MODE = "hbase.oceanbase.odpMode";
    public static final String HBASE_OCEANBASE_DATABASE = "hbase.oceanbase.database";

    private static final String OH_CONNECTION_IMPL = "com.alipay.oceanbase.hbase.util.OHConnectionImpl";

    /**
     * 创建基础 Configuration，并指定 OBKV-HBase 连接实现。
     * 若 classpath 下有 hbase-site.xml 会一并加载。
     */
    public static Configuration create() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.client.connection.impl", OH_CONNECTION_IMPL);
        return conf;
    }

    /**
     * 直连模式（私有化部署）。
     * fullUserName 格式：userName@tenantName#clusterName
     */
    public static Configuration forDirect(
            String paramURL,
            String fullUserName,
            String password,
            String sysUserName,
            String sysPassword) {
        Configuration conf = create();
        conf.set(HBASE_OCEANBASE_PARAM_URL, paramURL);
        conf.set(HBASE_OCEANBASE_FULL_USER_NAME, fullUserName);
        conf.set(HBASE_OCEANBASE_PASSWORD, password != null ? password : "");
        conf.set(HBASE_OCEANBASE_SYS_USER_NAME, sysUserName);
        conf.set(HBASE_OCEANBASE_SYS_PASSWORD, sysPassword != null ? sysPassword : "");
        return conf;
    }

    /**
     * 云上模式（公有云）。OBKV 端口固定 3307。
     */
    public static Configuration forCloud(
            String fullUserName,
            String password,
            String odpAddr,
            int odpPort,
            String database) {
        Configuration conf = create();
        conf.set(HBASE_OCEANBASE_FULL_USER_NAME, fullUserName);
        conf.set(HBASE_OCEANBASE_PASSWORD, password != null ? password : "");
        conf.set(HBASE_OCEANBASE_ODP_ADDR, odpAddr);
        conf.setInt(HBASE_OCEANBASE_ODP_PORT, odpPort);
        conf.setBoolean(HBASE_OCEANBASE_ODP_MODE, true);
        conf.set(HBASE_OCEANBASE_DATABASE, database);
        return conf;
    }
}
