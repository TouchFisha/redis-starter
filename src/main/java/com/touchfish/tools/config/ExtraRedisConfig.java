package com.touchfish.tools.config;

import com.touchfish.tools.structure.ExtraRedisProperties;
import com.touchfish.tools.util.JedisUtil;
import com.touchfish.tools.util.RedisUtil;
import com.touchfish.tools.util.RedissonUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Data
@Slf4j
@Component
@Configuration
@ConfigurationProperties(prefix = "conf")
public class ExtraRedisConfig {
    private boolean loadExtraRedis = false;
    private boolean loadExtraJedis = false;
    private boolean loadExtraRedisson = false;
    private Map<String, ExtraRedisProperties> extraRedis = new HashMap<>();
    @Autowired
    public ApplicationContext appContext;
    public static Map<String, RedisUtil> redis;
    public static Map<String, RedissonUtil> redisson;
    public static Map<String, JedisUtil> jedis;
    @Bean("jedis")
    public Map<String, JedisUtil> jedis() {
        jedis = new HashMap<>();
        if (loadExtraJedis && !loadExtraRedis) {
            for (Map.Entry<String, ExtraRedisProperties> e : extraRedis.entrySet()) {
                jedisUtilRegister(e.getKey(), e.getValue(), (ConfigurableApplicationContext) appContext);
            }
            log.info("Extra Jedis Util Initialized.  Count: " + jedis.size());
        }
        return jedis;
    }
    @Bean("redis")
    public Map<String, RedisUtil> redis() {
        redis = new HashMap<>();
        if (loadExtraRedis) {
            for (Map.Entry<String, ExtraRedisProperties> e : extraRedis.entrySet()) {
                redisTemplateRegister(e.getKey(), e.getValue(), (ConfigurableApplicationContext) appContext);
            }
            log.info("Extra Redis Util Initialized.  Count: " + redis.size());
        }
        return redis;
    }
    @Bean("redisson")
    public Map<String, RedissonUtil> redisson() {
        redisson = new HashMap<>();
        if (loadExtraRedisson) {
            for (Map.Entry<String, ExtraRedisProperties> e : extraRedis.entrySet()) {
                redissonClientRegister(e.getKey(), e.getValue(), (ConfigurableApplicationContext) appContext);
            }
            log.info("Extra Redisson Util Initialized.  Count: " + redisson.size());
        }
        return redisson;
    }

    public JedisUtil jedisUtilRegister(String name, ExtraRedisProperties properties, ConfigurableApplicationContext configurableApplicationContext){
        JedisUtil bean = JedisUtil.builder()
                .address(properties.hostInfo)
                .password(properties.password)
                .timeout(properties.connectionTimeout)
                .database(properties.database)
                .master(properties.master)
                .type(properties.type).build(name);

        DefaultListableBeanFactory defaultListableBeanFactory = (DefaultListableBeanFactory) configurableApplicationContext.getAutowireCapableBeanFactory();
        if (!defaultListableBeanFactory.containsBeanDefinition(name)) {
            defaultListableBeanFactory.registerSingleton(name, bean.connect());
        }
        jedis.put(name, bean);
        log.info("Extra Jedis \""+name+"\" Connected.");
        return bean;
    }

    public RedissonUtil redissonClientRegister(String name, ExtraRedisProperties properties, ConfigurableApplicationContext configurableApplicationContext){
        RedissonUtil bean = RedissonUtil.builder()
                .address(properties.hostInfo)
                .password(properties.password)
                .timeout(properties.connectionTimeout)
                .database(properties.database)
                .master(properties.master)
                .type(properties.type).build(name);

        DefaultListableBeanFactory defaultListableBeanFactory = (DefaultListableBeanFactory) configurableApplicationContext.getAutowireCapableBeanFactory();
        if (!defaultListableBeanFactory.containsBeanDefinition(name+"Redisson")) {
            defaultListableBeanFactory.registerSingleton(name+"Redisson", bean.connect());
        }
        redisson.put(name, bean);
        log.info("Extra Redisson \""+name+"\" Connected.");
        return bean;
    }
    public RedisUtil redisTemplateRegister(String name, ExtraRedisProperties properties, ConfigurableApplicationContext configurableApplicationContext){
        RedisUtil bean = RedisUtil.builder()
                .address(properties.hostInfo)
                .maxRedirects(properties.maxRedirects)
                .password(properties.password)
                .timeout(properties.connectionTimeout)
                .database(properties.database)
                .master(properties.master)
                .keySerializer(properties.keySerializer)
                .valueSerializer(properties.valueSerializer)
                .hashKeySerializer(properties.hashKeySerializer)
                .hashValueSerializer(properties.hashValueSerializer)
                .type(properties.type).build(name);

        DefaultListableBeanFactory defaultListableBeanFactory = (DefaultListableBeanFactory) configurableApplicationContext.getAutowireCapableBeanFactory();
        if (!defaultListableBeanFactory.containsBeanDefinition(name)) {
            defaultListableBeanFactory.registerSingleton(name, bean.connect());
        }
        redis.put(name, bean);
        log.info("Extra Redis \""+name+"\" Connected.");
        return bean;
    }
}