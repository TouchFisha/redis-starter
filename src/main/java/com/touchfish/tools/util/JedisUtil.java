package com.touchfish.tools.util;

import com.touchfish.tools.structure.IPFormat;
import com.touchfish.tools.structure.RedisType;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.*;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


@Slf4j
public class JedisUtil implements Closeable {
    public static Builder builder() {
        return new Builder(new JedisUtil());
    }
    public static class Builder {
        JedisUtil target;
        public Builder() {
            this.target = new JedisUtil();
        }
        public Builder(JedisUtil target) {
            this.target = target;
        }
        public Builder type(RedisType type) {
            if (type != null)
                target.type = type;
            return this;
        }
        public Builder address(String address) {
            if (address != null && !address.isEmpty())
                target.address = address;
            return this;
        }
        public Builder password(String password) {
            if (password != null && !password.isEmpty())
                target.password = password;
            return this;
        }
        public Builder timeout(long timeout) {
            target.timeout = timeout;
            return this;
        }
        public Builder timeout(String timeout) {
            if (timeout != null && !timeout.isEmpty())
                target.timeout = Long.parseLong(timeout);
            return this;
        }
        public Builder database(int database) {
            target.database = database;
            return this;
        }
        public Builder database(String database) {
            if (database != null && !database.isEmpty())
                target.database = Integer.parseInt(database);
            return this;
        }
        public Builder master(String master) {
            if (master != null && !master.isEmpty())
                target.master = master;
            return this;
        }
        public JedisUtil build(String name) {
            if (name != null && !name.isEmpty())
                target.name = name;
            target.init();
            return target;
        }
        public JedisUtil build() {
            return build("");
        }
    }
    private String name;
    private RedisType type = RedisType.NONE;
    private String master = "mymaster";
    private String address;
    private String password;
    private Long timeout = 3000L;
    private Integer database;
    private Closeable connection;
    private JedisPoolAbstract jedisPool;
    private HostAndPort[] hostAndPorts;
    public JedisUtil() {}
    public JedisUtil(String name, RedisType type, String address) {
        new Builder(this).type(type).address(address).build(name);
    }
    public JedisUtil(String name, RedisType type, String master, String address, String password, int timeout, int database) {
        new Builder(this).type(type).master(master).address(address).password(password).timeout(timeout).database(database).build(name);
    }
    public void init() {
        String[] addresses = address.replace(" ","").split(",");
        hostAndPorts = new HostAndPort[addresses.length];
        for (int i = 0; i < addresses.length; i++) {
            hostAndPorts[i] = AddressUtil.formatAddress(addresses[i]);
        }
        refresh(type);
    }
    /**
     * 重新建立连接
     */
    public void refresh() {
        refresh(type);
    }
    /**
     * 重新建立连接
     * @param type 链接redis类型
     */
    public void refresh(RedisType type) {
        this.type = RedisType.NONE;
        connect(type);
    }
    /**
     * cluster集群链接，返回JedisCluster对象
     * @return JedisCluster
     */
    public JedisCluster cluster() {
        if (type != RedisType.CLUSTER) {
            close();
            connection = new JedisCluster(new HashSet<>(Arrays.asList(hostAndPorts)), Math.toIntExact(timeout));
            type = RedisType.CLUSTER;
        }
        return (JedisCluster) connection;
    }
    /**
     * 单点独立链接，返回Jedis对象
     * @return Jedis
     */
    public Jedis standalone() {
        if (type != RedisType.STANDALONE) {
            close();
            if (hostAndPorts.length < 1) {
                System.err.println("未正确设置redis地址。");
                return null;
            }
            connection = new Jedis(hostAndPorts[0]);
            type = RedisType.STANDALONE;
        }
        return (Jedis) connection;
    }
    /**
     * 获取哨兵模式redis连接池
     * @return JedisSentinelPool
     */
    public JedisSentinelPool sentinelPool() {
        if (type != RedisType.SENTINEL) {
            close();
            jedisPool = new JedisSentinelPool(master, Arrays.stream(hostAndPorts).map(HostAndPort::toString).collect(Collectors.toSet()));
            type = RedisType.SENTINEL;
        }
        return (JedisSentinelPool) jedisPool;
    }
    /**
     * 哨兵集群模式，返回Master的Jedis对象
     * @return Jedis
     */
    public Jedis sentinel() {
        closeConnection();
        if (jedisPool == null) {
            sentinelPool();
        }
        connection = jedisPool.getResource();
        return (Jedis) connection;
    }
    /**
     * 哨兵集群模式，返回Master的Jedis对象
     * @return Jedis
     */
    public Jedis master() {
        return sentinel();
    }
    /**
     * 根据链接类型获取redis链接
     * @param type STANDALONE,CLUSTER,SENTINEL
     * @return Jedis, JedisCluster
     * @param <T>
     */
    public <T> T connect(RedisType type) {
        switch (type) {
            case NONE: case STANDALONE:
                return (T) standalone();
            case CLUSTER:
                return (T) cluster();
            case SENTINEL:
                return (T) sentinel();
        }
        return null;
    }
    /**
     * 根据链接类型和返回值类型获取redis链接
     * @param type STANDALONE,CLUSTER,SENTINEL
     * @param clazz Jedis, JedisCluster
     * @return Jedis, JedisCluster
     * @param <T>
     */
    public <T> T connect(RedisType type, Class<T> clazz) {
        return connect(type);
    }
    /**
     * 获取当前类型的redis链接
     * @return Jedis, JedisCluster
     * @param <T>
     */
    public <T> T connect() {
        return connect(type);
    }
    /**
     * 根据当前类型和返回值类型获取redis链接
     * @param clazz Jedis, JedisCluster
     * @return Jedis, JedisCluster
     * @param <T>
     */
    public <T> T connect(Class<T> clazz) {
        return connect(type, clazz);
    }
    public boolean connectionTest() {
        Object connect = connect();
        if (connect.getClass() == Jedis.class) {
            return ((Jedis)connect).ping().contains("PONG");
        } else if (connect.getClass() == JedisCluster.class) {
            boolean res = false;
            for (Map.Entry<String, JedisPool> poolEntry : ((JedisCluster) connect).getClusterNodes().entrySet()) {
                System.out.println("Trying to connect to: " + poolEntry.getKey());
                JedisPool jp = poolEntry.getValue();
                try {
                    String ping = jp.getResource().ping();
                    if (ping.contains("PONG")) {
                        res = true;
                    }
                    System.out.println("Node " + poolEntry.getKey() + " is running (" + ping + ")");
                } catch (Exception e) {
                    System.out.println("Cannot connect to node " + poolEntry.getKey());
                }
            }
            return res;
        }
        return false;
    }
    public String name() {
        return name;
    }
    public RedisType type() {
        return type;
    }
    public void type(RedisType type) {
        if (type != this.type) {
            refresh(type);
        }
    }
    public IPFormat ipFormat() {
        if (hostAndPorts.length > 0) {
            return AddressUtil.getIPFormat(hostAndPorts[0].toString());
        }
        return IPFormat.NONE;
    }
    public String get(String key) {
        Object redis = connect();
        try {
            return (String) redis.getClass().getDeclaredMethod("get", String.class).invoke(redis, key);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public void set(String key, Object ... value) {
        Object redis = connect();
        List<Object> args = new ArrayList<>();
        args.add(key);
        args.addAll(Arrays.asList(value));
        try {
            redis.getClass().getDeclaredMethod("set", args.stream().map(Object::getClass).collect(Collectors.toList()).toArray(new Class[0])).invoke(redis, args.toArray());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public Long del(String key) {
        Object redis = connect();
        try {
            return (Long) redis.getClass().getDeclaredMethod("del", String.class).invoke(redis, key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            connection = null;
        }
    }
    /**
     * 关闭所有连接
     */
    public void close() {
        closeConnection();
        if (jedisPool != null) {
            jedisPool.destroy();
            jedisPool = null;
        }
    }
}
