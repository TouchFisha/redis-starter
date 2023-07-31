package com.touchfish.tools.util;

import com.touchfish.tools.config.ExtraRedisConfig;
import com.touchfish.tools.interf.IRedisConnection;
import com.touchfish.tools.structure.ExtraRedisProperties;
import com.touchfish.tools.structure.IPFormat;
import com.touchfish.tools.structure.RedisFactoryType;
import com.touchfish.tools.structure.RedisType;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
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
import redis.clients.jedis.JedisPoolConfig;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;


@Slf4j
public class RedisUtil implements IRedisConnection<RedisTemplate, RedisConfiguration> {

    public static Builder builder(){
        return new Builder(new RedisUtil());
    }
    private String name;
    private RedisType type = RedisType.NONE;
    private RedisFactoryType factoryType = RedisFactoryType.JEDIS;
    private String master = "mymaster";
    private String address;
    private String password;
    private Long timeout = 3000L;
    private Integer database;
    private Integer maxRedirects;
    private HostAndPort[] hostAndPorts;
    private RedisProperties.Pool poolProperties;
    private RedisTemplate<String, String> template;
    private RedisSerializer<?> keySerializer;
    private RedisSerializer<?> valueSerializer;
    private RedisSerializer<?> hashKeySerializer;
    private RedisSerializer<?> hashValueSerializer;
    public RedisUtil() {}
    public RedisUtil(String name, RedisType type, String address) {
        this.name = name;
        this.type = type;
        this.address = address;
        init();
    }
    public RedisUtil(String name, RedisType type, String master, String address, String password, Long timeout, Integer database) {
        this.name = name;
        this.type = type;
        this.master = master;
        this.address = address;
        this.password = password;
        this.timeout = timeout;
        this.database = database;
        init();
    }
    public boolean init() {
        String[] addresses = address.replace(" ","").split(",");
        hostAndPorts = new HostAndPort[addresses.length];
        for (int i = 0; i < addresses.length; i++) {
            hostAndPorts[i] = AddressUtil.formatAddress(addresses[i]);
        }
        refresh(type);
        return connectionTest();
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
     * cluster集群链接
     * @return
     */
    public RedisTemplate cluster() {
        if (type != RedisType.CLUSTER) {
            close();
            type = RedisType.CLUSTER;
            template = create(config());
        }
        return template;
    }
    /**
     * 单点独立链接
     * @return
     */
    public RedisTemplate standalone() {
        if (type != RedisType.STANDALONE) {
            close();
            type = RedisType.STANDALONE;
            template = create(config());
        }
        return template;
    }

    /**
     * 哨兵集群模式
     * @return
     */
    public RedisTemplate sentinel() {
        if (type != RedisType.SENTINEL) {
            close();
            type = RedisType.SENTINEL;
            template = create(config());
        }
        return template;
    }

    @Override
    public RedisConfiguration config() {
        return config(type);
    }

    @Override
    public RedisConfiguration config(RedisType redisType) {
        switch (type) {
            case STANDALONE:
                return standaloneConfig();
            case CLUSTER:
                return clusterConfig();
            case SENTINEL:
                return sentinelConfig();
        }
        return null;
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
     * 根据类型获取连接
     * @param type STANDALONE,CLUSTER,SENTINEL
     * @return
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

    @Override
    public RedisTemplate create(RedisConfiguration configuration) {
        RedisConnectionFactory factory = factory(configuration, timeout, factoryType, poolProperties);
        log.info("Redis \""+name+"\" Factory Created: " + factory.getClass().getSimpleName());
        if (factory != null) {
            return create(factory);
        }
        return null;
    }
    private RedisTemplate create(RedisConnectionFactory connectionFactory){
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

    /**
     * 获取当前类型的连接
     * @return
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
        if (hostAndPorts.length > 0) {
            return AddressUtil.getIPFormat(hostAndPorts[0].toString());
        }
        return IPFormat.NONE;
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

    /**
     * 关闭所有连接
     */
    public void close() {
        template = null;
    }

    public static <T extends GenericObjectPoolConfig> T poolConfig(T poolConfig, RedisProperties.Pool pool) {
        if (pool != null) {
            poolConfig.setMaxIdle(pool.getMaxActive());
            poolConfig.setMaxIdle(pool.getMaxIdle());
            poolConfig.setMinIdle(pool.getMinIdle());
            if (pool.getMaxWait() != null) {
                poolConfig.setMaxWaitMillis(pool.getMaxWait().toMillis());
            }
            if (pool.getTimeBetweenEvictionRuns() != null) {
                poolConfig.setTimeBetweenEvictionRunsMillis(pool.getTimeBetweenEvictionRuns().toMillis());
            }
        }
        return poolConfig;
    }
    public static RedisConnectionFactory lettuceFactory(RedisConfiguration config, Long timeout, RedisProperties.Pool poolProps){
        LettucePoolingClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(timeout))
                .poolConfig(poolConfig(new GenericObjectPoolConfig(), poolProps))
                .build();
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(config, clientConfig);
        //如果要使pool参数生效,一定要关闭shareNativeConnection
        //connectionFactory.setShareNativeConnection(false);
        //必须初始化实例
        connectionFactory.afterPropertiesSet();
        return connectionFactory;
    }
    private static RedisConnectionFactory jedisFactory(RedisConfiguration config, Long timeout, RedisProperties.Pool poolProps){
        JedisConnectionFactory factory = null;
        if (config.getClass() == RedisClusterConfiguration.class) {
            factory = new JedisConnectionFactory((RedisClusterConfiguration) config, poolConfig(new JedisPoolConfig(), poolProps));
        } else if (config.getClass() == RedisStandaloneConfiguration.class) {
            factory = new JedisConnectionFactory((RedisStandaloneConfiguration) config);
        } else if (config.getClass() == RedisSentinelConfiguration.class) {
            factory = new JedisConnectionFactory((RedisSentinelConfiguration) config, poolConfig(new JedisPoolConfig(), poolProps));
        }
        if (factory != null) {
            if (timeout != null) {
                factory.setTimeout(Math.toIntExact(timeout));
            }
            factory.afterPropertiesSet();
        }
        return factory;
    }
    public static RedisConnectionFactory factory(RedisConfiguration config, Long timeout, RedisFactoryType type, RedisProperties.Pool poolProps){
        switch (type) {
            case JEDIS:
                return jedisFactory(config, timeout, poolProps);
            case LETTUCE:
                return lettuceFactory(config, timeout, poolProps);
        }
        return null;
    }
    public static ExtraRedisProperties translateToExtraProperties(RedisProperties properties) {
        ExtraRedisProperties res = new ExtraRedisProperties();
        if (properties != null) {
            if (properties.getTimeout() != null) {
                res.connectionTimeout = String.valueOf(properties.getTimeout().toMillis());
            }
            res.password = properties.getPassword();
            if (properties.getCluster() != null) {
                RedisProperties.Cluster cluster = properties.getCluster();
                res.type = RedisType.CLUSTER;
                res.maxRedirects = String.valueOf(cluster.getMaxRedirects());
                res.hostInfo = "";
                for (String node : cluster.getNodes()) {
                    res.hostInfo += node + ",";
                }
            } else if (properties.getSentinel() != null) {
                res.type = RedisType.SENTINEL;
                RedisProperties.Sentinel sentinel = properties.getSentinel();
                res.setMaxRedirects(sentinel.getMaster());
                res.hostInfo = "";
                for (String node : sentinel.getNodes()) {
                    res.hostInfo += node + ",";
                }
            } else if (properties.getHost() != null) {
                res.type = RedisType.STANDALONE;
                res.setHostInfo(properties.getHost()+":"+properties.getPort());
            }
        }
        return res;
    }
    public static RedisProperties translateToRedisProperties(ExtraRedisProperties properties) {
        RedisProperties res = new RedisProperties();
        if (properties != null) {
            if (properties.getConnectionTimeout() != null && !properties.getConnectionTimeout().isEmpty()) {
                res.setTimeout(Duration.ofMillis(Long.parseLong(properties.getConnectionTimeout())));
            }
            if (properties.getDatabase() != null && !properties.getDatabase().isEmpty()) {
                res.setDatabase(Integer.parseInt(properties.getDatabase()));
            }
            if (properties.getPassword() != null && !properties.getPassword().isEmpty()) {
                res.setPassword(properties.getPassword());
            }
            if (properties.getType() == RedisType.CLUSTER) {
                RedisProperties.Cluster cluster = new RedisProperties.Cluster();
                if (properties.getMaxRedirects() != null && !properties.getMaxRedirects().isEmpty()) {
                    cluster.setMaxRedirects(Integer.valueOf(properties.getMaxRedirects()));
                }
                List<String> nodes = new ArrayList<>(Arrays.asList(properties.getHostInfo().split(",")));
                cluster.setNodes(nodes);
                res.setCluster(cluster);
            } else if (properties.getType() == RedisType.SENTINEL) {
                RedisProperties.Sentinel sentinel = new RedisProperties.Sentinel();
                sentinel.setMaster(properties.getMaster());
                List<String> nodes = new ArrayList<>(Arrays.asList(properties.getHostInfo().split(",")));
                sentinel.setNodes(nodes);
                res.setSentinel(sentinel);
            } else if (properties.getHostInfo() != null && !properties.getHostInfo().isEmpty()) {
                HostAndPort hostAndPort = AddressUtil.formatAddress(properties.getHostInfo().split(",")[0]);
                res.setHost(hostAndPort.getHost());
                res.setPort(hostAndPort.getPort());
            }
        }
        return res;
    }
    public static RedisConfiguration config(RedisProperties properties) {
        if (properties != null) {
            if (properties.getCluster() != null) {
                RedisProperties.Cluster cluster = properties.getCluster();
                RedisClusterConfiguration config=new RedisClusterConfiguration();
                for (String node : cluster.getNodes()) {
                    HostAndPort hostAndPort = AddressUtil.formatAddress(node);
                    RedisNode redisNode = new RedisNode(hostAndPort.getHost(),hostAndPort.getPort());
                    config.addClusterNode(redisNode);
                }
                if (properties.getPassword() != null){
                    config.setPassword(properties.getPassword());
                }
                if (cluster.getMaxRedirects() != null) {
                    config.setMaxRedirects(cluster.getMaxRedirects());
                }
                return config;
            } else if (properties.getSentinel() != null) {
                RedisProperties.Sentinel sentinel = properties.getSentinel();
                RedisSentinelConfiguration config = new RedisSentinelConfiguration();
                config.master(sentinel.getMaster());
                List<RedisNode> nodes = new ArrayList<>();
                for (String node : sentinel.getNodes()) {
                    HostAndPort hostAndPort = AddressUtil.formatAddress(node);
                    RedisNode redisNode=new RedisNode(hostAndPort.getHost(),hostAndPort.getPort());
                    nodes.add(redisNode);
                }
                config.setSentinels(nodes);
                if (properties.getPassword() != null){
                    config.setPassword(properties.getPassword());
                }
                return config;
            } else if (properties.getHost() != null) {
                RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
                config.setHostName(properties.getHost());
                config.setPort(properties.getPort());
                config.setPassword(properties.getPassword());
                return config;
            }
        }
        return null;
    }

    public static class Builder {
        private RedisUtil target;
        public Builder(RedisUtil target) {
            this.target = target;
        }
        public Builder type(RedisType type) {
            if (type != null)
                target.type = type;
            return this;
        }
        public Builder address(String address) {
            if (address != null && !address.isEmpty())
                address = address.replace(" ", "");
                target.address = address.endsWith(",") ? address.substring(0, address.length()-1) : address;
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
        public Builder factory(RedisFactoryType factory) {
            if (factory != null)
                target.factoryType = factory;
            return this;
        }
        public Builder pool(RedisProperties.Pool pool) {
            if (pool != null)
                target.poolProperties = pool;
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
        public RedisUtil build(){
            return build("");
        }
        public RedisUtil build(String name){
            if (name != null && !name.isEmpty())
                target.name = name;
            else target.name = "New RedisUtil " + UUID.randomUUID();
            target.init();
            return target;
        }
    }
}