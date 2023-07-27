package com.touchfish.tools.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ：LiuShihao
 * @date ：Created in 2021/11/22 3:52 下午
 * @desc ：Redis配置类
 * org.springframework.boot.autoconfigure.data.redis.RedisProperties 会根据配置自动加载为一个bean
 */
@Slf4j
@Configuration
public class RedisConfig {

    /**
     * @param properties  org.springframework.boot.autoconfigure.data.redis.RedisProperties
     *
     * 如果使用的是直接连接redis的方式，即每次连接都创建新的连接。当并发量剧增时，这会带来性能上开销，同时由于没有对连接数进行限制，则可能使服务器崩溃导致无法响应。
     * 所以我们一般都会建立连接池，事先初始化一组连接，供需要redis连接的线程取用。
     *
     * 使用RedisStandaloneConfiguration、RedisSentinelConfiguration、RedisClusterConfiguration三种方式配置连接信息。
     *
     * @return
     */
    @Bean(name = "RedisConnectionFactory")
    public RedisConnectionFactory connectionFactory(RedisProperties properties){
        //Jedis连接工厂 JedisConnectionFactory是RedisConnectionFactory子类
        JedisConnectionFactory factory;
        //RedisSentinelConfiguration 是 RedisConfiguration 的子类
        RedisSentinelConfiguration sentinelConfig = getSentinelConfiguration(properties);
        RedisClusterConfiguration clusterConfiguration = getClusterConfiguration(properties);
        if (sentinelConfig != null) {
            log.info("Redis Sentinel 集群配置...");
            factory = new JedisConnectionFactory(sentinelConfig);
        } else if (clusterConfiguration != null) {
            log.info("Redis Cluster 集群配置...");
            factory = new JedisConnectionFactory(clusterConfiguration);
        } else {
            log.info("Redis Standalone单节点配置...");
            RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration();
            redisStandaloneConfiguration.setHostName(properties.getHost());
            redisStandaloneConfiguration.setPort(properties.getPort());
            redisStandaloneConfiguration.setPassword(properties.getPassword());
            factory = new JedisConnectionFactory(redisStandaloneConfiguration);
        }

        return factory;
    }

    /**
     * 主从集群信息配置
     * @param properties
     * @return
     */
    private RedisClusterConfiguration getClusterConfiguration(RedisProperties properties) {
        RedisProperties.Cluster cluster = properties.getCluster();
        if (cluster != null) {
            RedisClusterConfiguration config=new RedisClusterConfiguration();
            for (String node : cluster.getNodes()) {
                HostAndPort hostAndPort = HostAndPort.fromString(node);
                RedisNode redisNode=new RedisNode(hostAndPort.getHost(),hostAndPort.getPort());
                config.addClusterNode(redisNode);
            }
            if (properties.getPassword() != null){
                config.setPassword(properties.getPassword());
            }
            if (cluster.getMaxRedirects() != null) {
                config.setMaxRedirects(cluster.getMaxRedirects());
            }
            return config;
        }
        return null;
    }

    /**
     * 哨兵集群信息配置
     * @param properties
     * @return
     */
    private RedisSentinelConfiguration getSentinelConfiguration(RedisProperties properties) {
        RedisProperties.Sentinel sentinel = properties.getSentinel();
        if (sentinel != null) {
            RedisSentinelConfiguration config = new RedisSentinelConfiguration();
            config.master(sentinel.getMaster());
            config.setSentinels(createSentinels(sentinel));
            if (properties.getPassword() != null){
                config.setPassword(properties.getPassword());
            }
            return config;
        }
        return null;
    }

    /**
     * 创建哨兵集群节点
     * @param sentinel
     * @return
     */
    private static List<RedisNode> createSentinels(RedisProperties.Sentinel sentinel) {
        List<RedisNode> nodes = new ArrayList<>();
        for (String node : sentinel.getNodes()) {
            HostAndPort hostAndPort = HostAndPort.fromString(node);
            RedisNode redisNode=new RedisNode(hostAndPort.getHost(),hostAndPort.getPort());
            nodes.add(redisNode);
        }
        return nodes;
    }

    @Bean
    public RedisTemplate redisTemplate(@Qualifier("RedisConnectionFactory") RedisConnectionFactory redisConnectionFactory) {
        // 设置序列化
        Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        jackson2JsonRedisSerializer.setObjectMapper(om);
        // 配置redisTemplate
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        RedisSerializer<?> stringSerializer = new StringRedisSerializer();
        // key序列化
        redisTemplate.setKeySerializer(stringSerializer);
        // value序列化
        redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);
        // Hash key序列化
        redisTemplate.setHashKeySerializer(stringSerializer);
        // Hash value序列化
        redisTemplate.setHashValueSerializer(jackson2JsonRedisSerializer);
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }
    @Bean(name = "StringRedisTemplate")
    public StringRedisTemplate StringRedisTemplate(@Qualifier("RedisConnectionFactory") RedisConnectionFactory redisConnectionFactory) {

        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate(redisConnectionFactory);

        return stringRedisTemplate;
    }

    /**
     *
     * 使用
     * 连接池配置
     * @param maxTotal
     * @param maxWaitMillis
     * @param maxIdle
     * @return
     */
    @Bean(name = "JedisPoolConfig")
    public JedisPoolConfig jedisPoolConfig(@Value("${jedis.pool.config.maxTotal:100}") int maxTotal,
                                           @Value("${jedis.pool.config.maxWaitMillis:5000}") int maxWaitMillis,
                                           @Value("${jedis.pool.config.maxIdle:10}") int maxIdle) {
        log.info("Redis连接池配置...");
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMaxWaitMillis(maxWaitMillis);
        return config;
    }

}

