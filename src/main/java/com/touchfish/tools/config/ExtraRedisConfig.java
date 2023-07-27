package com.touchfish.tools.config;

import com.touchfish.tools.structure.ExtraRedisProperties;
import com.touchfish.tools.util.ExtraRedisUtil;
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
    private boolean loadExtraRedis = true;
    private Map<String, ExtraRedisProperties> extraRedis = new HashMap<>();
    @Autowired
    public ApplicationContext appContext;
    public static Map<String, ExtraRedisUtil> redis;
    @Bean("redis")
    public Map<String, ExtraRedisUtil> redis() {
        redis = new HashMap<>();
        if (loadExtraRedis) {
            for (Map.Entry<String, ExtraRedisProperties> e : extraRedis.entrySet()) {
                register(e.getKey(), e.getValue(), (ConfigurableApplicationContext) appContext);
            }
        }
        log.info("Extra Redis Util Initialized.  Count: " + redis.size());
        return redis;
    }
    public ExtraRedisUtil register(String name, ExtraRedisProperties properties, ConfigurableApplicationContext configurableApplicationContext){
        ExtraRedisUtil redisUtil = ExtraRedisUtil.builder()
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
            defaultListableBeanFactory.registerSingleton(name, redisUtil.connect());
        }
        redis.put(name, redisUtil);
        log.info("Extra Redis \""+name+"\" Connected.");
        return redisUtil;
    }
}