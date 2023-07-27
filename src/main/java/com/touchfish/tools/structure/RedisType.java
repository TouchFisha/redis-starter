package com.touchfish.tools.structure;

public enum RedisType {
    /**
     * 未连接
     */
    NONE,
    /**
     * 单点独立链接，返回Jedis对象
     */
    STANDALONE,
    /**
     * cluster集群链接，返回JedisCluster对象
     */
    CLUSTER,
    /**
     * 哨兵集群模式，创建哨兵连接池并返回Master的Jedis对象
     */
    SENTINEL
}