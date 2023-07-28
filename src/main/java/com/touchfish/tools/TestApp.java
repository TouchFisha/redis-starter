package com.touchfish.tools;

import org.redisson.api.RedissonClient;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.redis.core.RedisTemplate;

@SpringBootApplication
public class TestApp implements ApplicationContextAware, CommandLineRunner {
    private ApplicationContext applicationContext;
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(TestApp.class);
        application.run(args);
    }
    public void test(String redisName, String setKey, String setValue){
        applicationContext.getBean(redisName, RedisTemplate.class).opsForValue().set(setKey,setValue);
        for (String s : applicationContext.getBean(redisName+"Redisson", RedissonClient.class).getKeys().getKeys()) {
            System.out.println(s);
            try {
                System.out.println(applicationContext.getBean(redisName, RedisTemplate.class).opsForValue().get(s));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        applicationContext.getBean(redisName, RedisTemplate.class).delete(setKey);
    }
    @Override
    public void setApplicationContext(ApplicationContext arg0) {
        this.applicationContext = arg0;
    }
    @Override
    public void run(String... args) throws Exception {
//        test("name0","lalala0","123123");
//        test("name1","lalala1","2333");
//        test("name2","lalala2","612313");
    }
}
