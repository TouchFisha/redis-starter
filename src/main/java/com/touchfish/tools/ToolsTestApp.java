package com.touchfish.tools;

import com.touchfish.tools.structure.RedisType;
import com.touchfish.tools.util.JedisUtil;
import com.touchfish.tools.util.RedisUtil;
import com.touchfish.tools.util.RedissonUtil;
import org.redisson.api.RedissonClient;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.Jedis;

import javax.annotation.Resource;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@SpringBootApplication
public class ToolsTestApp implements ApplicationContextAware, CommandLineRunner {



    private ApplicationContext applicationContext;
    public static Map<String,Object> controllers;
    public static Map<String,Object> services;

    @Override
    public void setApplicationContext(ApplicationContext arg0) {
        this.applicationContext = arg0;
    }

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(ToolsTestApp.class);
        application.run(args);
    }

    @Override
    public void run(String... args) throws Exception {
//        applicationContext.getBean("main", RedisTemplate.class).opsForValue().set("sb","123213");
//        applicationContext.getBean("main", RedisTemplate.class).opsForValue().set("jedis","SBBBBB");
//        for (String s : applicationContext.getBean("mainRedisson", RedissonClient.class).getKeys().getKeys()) {
//            System.out.println(applicationContext.getBean("main", RedisTemplate.class).opsForValue().get(s));
//            System.out.println(applicationContext.getBean("main", RedisTemplate.class).delete(s));
//        }
//        applicationContext.getBean("main", Jedis.class).set("sb","123213");
//        applicationContext.getBean("main", Jedis.class).set("jedis","SBBBBB");
//        for (String s : applicationContext.getBean("mainRedisson", RedissonClient.class).getKeys().getKeys()) {
//            System.out.println(applicationContext.getBean("main", Jedis.class).get(s));
//            System.out.println(applicationContext.getBean("main", Jedis.class).del(s));
//        }
    }
}
