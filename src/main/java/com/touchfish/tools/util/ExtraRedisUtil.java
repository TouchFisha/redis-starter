package com.touchfish.tools.util;

import com.touchfish.tools.structure.IPFormat;
import com.touchfish.tools.structure.RedisFactoryType;
import com.touchfish.tools.structure.RedisType;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.HostAndPort;

import java.time.Duration;
import java.util.*;

@Slf4j
public class ExtraRedisUtil {
    public final RedisFactoryType factoryType = RedisFactoryType.JEDIS;
    public static Builder builder(){
        return new Builder(new ExtraRedisUtil());
    }
    public static class Builder {
        private ExtraRedisUtil target;
        public Builder(ExtraRedisUtil target) {
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
        public Builder maxRedirects(String maxRedirects) {
            if (maxRedirects != null && !maxRedirects.isEmpty())
                target.maxRedirects = Integer.valueOf(maxRedirects);
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
                target.timeout = Long.valueOf(timeout);
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
        private RedisSerializer serializer(String clazz) {
            RedisSerializer serializer = null;
            if (clazz.contains("StringRedisSerializer")) {
                serializer = new StringRedisSerializer();
            } else {
                Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);
                ObjectMapper om = new ObjectMapper();
                om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
                om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
                jackson2JsonRedisSerializer.setObjectMapper(om);
                serializer = jackson2JsonRedisSerializer;
            }
            return serializer;
        }
        public Builder keySerializer(String clazz) {
            if (clazz == null || clazz.isEmpty()) clazz = "StringRedisSerializer";
            target.keySerializer = serializer(clazz);
            return this;
        }
        public Builder valueSerializer(String clazz) {
            if (clazz == null || clazz.isEmpty()) clazz = "Jackson2JsonRedisSerializer";
            target.valueSerializer = serializer(clazz);
            return this;
        }
        public Builder hashKeySerializer(String clazz) {
            if (clazz == null || clazz.isEmpty()) clazz = "StringRedisSerializer";
            target.hashKeySerializer = serializer(clazz);
            return this;
        }
        public Builder hashValueSerializer(String clazz) {
            if (clazz == null || clazz.isEmpty()) clazz = "Jackson2JsonRedisSerializer";
            target.hashValueSerializer = serializer(clazz);
            return this;
        }
        public ExtraRedisUtil build(){
            return build("");
        }
        public ExtraRedisUtil build(String name){
            target.name = name;
            target.init();
            return target;
        }
    }
    private String name;
    private RedisType type = RedisType.NONE;
    private IPFormat ipFormat;
    private String master = "mymaster";
    private String address;
    private String password;
    private Long timeout = 3000L;
    private Integer database;
    private Integer maxRedirects;
    private HostAndPort[] hostAndPorts;
    private RedisTemplate<String, String> template;
    private RedisSerializer<?> keySerializer;
    private RedisSerializer<?> valueSerializer;
    private RedisSerializer<?> hashKeySerializer;
    private RedisSerializer<?> hashValueSerializer;
    public ExtraRedisUtil() {}
    public ExtraRedisUtil(String name, RedisType type, String address) {
        this.name = name;
        this.type = type;
        this.address = address;
        init();
    }
    public ExtraRedisUtil(String name, RedisType type, String master, String address, String password, Long timeout, Integer database) {
        this.name = name;
        this.type = type;
        this.master = master;
        this.address = address;
        this.password = password;
        this.timeout = timeout;
        this.database = database;
        init();
    }
    public void init() {
        String[] addresses = address.replace(" ","").split(",");
        hostAndPorts = new HostAndPort[addresses.length];
        for (int i = 0; i < addresses.length; i++) {
            hostAndPorts[i] = formatAddress(addresses[i]);
        }
        refresh(type);
        connectionTest();
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
    public RedisConfiguration clusterConfig() {
        RedisClusterConfiguration config = new RedisClusterConfiguration();
        for (HostAndPort hostAndPort : hostAndPorts) {
            RedisNode redisNode = new RedisNode(hostAndPort.getHost(), hostAndPort.getPort());
            config.addClusterNode(redisNode);
            log.info("Cluster Node: "+hostAndPort);
        }
        if (maxRedirects != null) {
            config.setMaxRedirects(maxRedirects);
        }
        if(password != null && !password.isEmpty()) {
            config.setPassword(password);
        }
        return config;
    }
    /**
     * cluster集群链接，返回JedisCluster对象
     * @return JedisCluster
     */
    public RedisTemplate cluster() {
        if (type != RedisType.CLUSTER) {
            close();
            template = createRedisTemplate(clusterConfig(), timeout);
            type = RedisType.CLUSTER;
        }
        return template;
    }
    public RedisConfiguration standaloneConfig() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        for (HostAndPort hostAndPort : hostAndPorts) {
            config.setHostName(hostAndPort.getHost());
            config.setPort(hostAndPort.getPort());
            log.info("Standalone: "+hostAndPort);
        }
        if(password != null && !password.isEmpty()) {
            config.setPassword(password);
        }
        if (database != null) {
            config.setDatabase(database);
        }
        return config;
    }
    /**
     * 单点独立链接，返回Jedis对象
     * @return Jedis
     */
    public RedisTemplate standalone() {
        if (type != RedisType.STANDALONE) {
            close();
            template = createRedisTemplate(standaloneConfig(), timeout);
            type = RedisType.STANDALONE;
        }
        return template;
    }
    public RedisConfiguration sentinelConfig() {
        RedisSentinelConfiguration config = new RedisSentinelConfiguration();
        config.setMaster(master);
        for (HostAndPort hostAndPort : hostAndPorts) {
            RedisNode redisNode = new RedisNode(hostAndPort.getHost(), hostAndPort.getPort());
            config.addSentinel(redisNode);
            log.info("Sentinel Node: "+hostAndPort);
        }
        if(password != null && !password.isEmpty()) {
            config.setPassword(password);
        }
        if (database != null) {
            config.setDatabase(database);
        }
        return config;
    }
    /**
     * 哨兵集群模式，返回Master的Jedis对象
     * @return Jedis
     */
    public RedisTemplate sentinel() {
        if (type != RedisType.SENTINEL) {
            close();
            template = createRedisTemplate(sentinelConfig(), timeout);
            type = RedisType.SENTINEL;
        }
        return template;
    }
    /**
     * 根据类型获取redis链接
     * @param type STANDALONE,CLUSTER,SENTINEL
     * @return Jedis, JedisCluster
     */
    public RedisTemplate connect(RedisType type) {
        log.info("Try Access \""+name+"\" "+type+" RedisTemplate.");
        switch (type) {
            case NONE: case STANDALONE:
                return standalone();
            case CLUSTER:
                return cluster();
            case SENTINEL:
                return sentinel();
        }
        return null;
    }
    /**
     * 获取当前类型的redis链接
     * @return Jedis, JedisCluster
     */
    public RedisTemplate connect() {
        return connect(type);
    }

    public boolean connectionTest() {
        boolean res = false;
        try {
            res = Objects.equals(connect().execute(new RedisCallback<String>() {
                public String doInRedis(RedisConnection connection) {
                    return connection.ping();
                }
            }), "PONG");
        } catch (Exception e) { e.printStackTrace(); }
        if (res) {
            log.info("Redis \""+name+"\" Successfully Connected.");
        } else {
            log.error("Redis \""+name+"\" Connect Failed.");
        }
        return res;
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
        return ipFormat;
    }
    public Object get(String key) {
        try {
            return connect().opsForValue().get(key);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public void set(String key, Object value) {
        try {
            connect().opsForValue().set(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public Boolean del(String key) {
        try {
            return connect().delete(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    private RedisTemplate<String, String> createRedisTemplate(RedisConfiguration config, Long timeout) {
        RedisConnectionFactory factory = null;
        switch (factoryType) {
            case JEDIS:
                factory = createJedisFactory(config, timeout);
                break;
            case LETTUCE:
                factory = createLettuceFactory(config, timeout);
                break;
            default:
                log.error("Unexpected value: " + factoryType);
                break;
        }
        log.info("Redis \""+name+"\" Factory Created: " + factory.getClass().getSimpleName());
        if (factory != null) {
            return createRedisTemplate(factory);
        }
        return null;
    }
    private RedisTemplate createRedisTemplate(RedisConnectionFactory connectionFactory){
        RedisTemplate template = new RedisTemplate();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(keySerializer);
        template.setValueSerializer(valueSerializer);
        template.setHashKeySerializer(hashKeySerializer);
        template.setHashValueSerializer(hashValueSerializer);
        template.afterPropertiesSet();
        log.info("Redis \""+name+"\" Template Created: " + template.getClass().getSimpleName());
        return template;
    }
    public static RedisConnectionFactory createLettuceFactory(RedisConfiguration config, Long timeout){
        LettucePoolingClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(timeout))
                .poolConfig(new GenericObjectPoolConfig())
                .build();
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config, clientConfig);
        //如果要使pool参数生效,一定要关闭shareNativeConnection
        //connectionFactory.setShareNativeConnection(false);
        //必须初始化实例
        connectionFactory.afterPropertiesSet();
        return connectionFactory;
    }
    private static RedisConnectionFactory createJedisFactory(RedisConfiguration config, Long timeout){
        JedisConnectionFactory factory = null;
        if (config.getClass() == RedisClusterConfiguration.class) {
            factory = new JedisConnectionFactory((RedisClusterConfiguration) config);
        } else if (config.getClass() == RedisStandaloneConfiguration.class) {
            factory = new JedisConnectionFactory((RedisStandaloneConfiguration) config);
        } else if (config.getClass() == RedisSentinelConfiguration.class) {
            factory = new JedisConnectionFactory((RedisSentinelConfiguration) config);
        }
        if (factory != null) {
            if (timeout != null) {
                factory.setTimeout(Math.toIntExact(timeout));
            }
            factory.afterPropertiesSet();
        }
        return factory;
    }
    public HostAndPort formatAddress(String address) {
        if (address.contains("[") || address.chars().filter(c -> c == ':').count() > 1) {
            ipFormat = IPFormat.IPV6;
        } else {
            ipFormat = IPFormat.IPV4;
        }
        address = address.replace("[", "").replace("]", "");
        String host;
        String port;
        if (address.contains(":")){
            host = address.substring(0, address.lastIndexOf(":"));
            port = address.substring(address.lastIndexOf(":") + 1);
        } else {
            host = address;
            port = "";
        }
        return new HostAndPort(host, Integer.parseInt(port));
    }
    /**
     * 关闭所有连接
     */
    public void close() {
        template = null;
    }
}