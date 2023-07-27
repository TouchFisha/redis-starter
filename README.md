
By Touchfish
***
# Redis Spring Boot Starter

- #### 在application.yml写入对应配置项
```yml
# application.yml
conf:
  # 启用时，自动注入所有根据extraRedis配置的RedisTemplate对象。
  loadExtraRedis: true
  # 启用时，自动注入所有根据extraRedis配置的Jedis及JedisCluster对象。 
  # !!! 与loadExtraRedis冲突
  loadExtraJedis: false
  # 启用时，自动注入所有根据extraRedis配置的Redisson对象。
  loadExtraRedisson: true
  extraRedis:
    name0:
      # 必填 地址，支持IPv4和IPv6
      hostInfo: '::1:7001, 10.28.198.52:7002, 10.28.198.53:7002'
      # 可选 密码
      # password: xxxx
      # 可选 连接超时时间 单位 ms 默认 3000
      connectionTimeout: 3000
      # 可选 连接类型 支持 CLUSTER, STANDALONE, SENTINEL 默认 STANDALONE
      type: CLUSTER
      # 可选 数据库索引
      # database: 0
      # 可选 哨兵模式主服务名称 默认 mymaster
      # master: mymaster
      # 可选 最大重定向次数 默认 mymaster
      # maxRedirects: 10
      # 以下都可选 序列化方式 当前为默认值
      keySerializer: "StringRedisSerializer"
      valueSerializer: "Jackson2JsonRedisSerializer"
      hashKeySerializer: "StringRedisSerializer"
      hashValueSerializer: "Jackson2JsonRedisSerializer"
    name1:
      hostInfo: "::1:26379,::2:26379"
      master: mymaster
      type: SENTINEL
    name2:
      hostInfo: "::1:6379"
      database: 0
      type: STANDALONE
```
- #### 通过SpringBoot获取对象
```java
    
    public ApplicationContext app;
    //loadExtraRedis: true
    app.getBean("name0", RedisTemplate.class);
    app.getBean("name1", RedisTemplate.class);
    app.getBean("name2", RedisTemplate.class);
    //loadExtraJedis: true
    app.getBean("name0", JedisCluster.class);
    app.getBean("name1", Jedis.class);
    app.getBean("name2", Jedis.class);
    //loadExtraRedisson: true
    app.getBean("name0Redisson", RedissonClient.class);
    app.getBean("name1Redisson", RedissonClient.class);
    app.getBean("name2Redisson", RedissonClient.class);
```
#### 或者通过Map获取工具
```java
    @Resource
    public Map<String,RedisUtil> redis;
    @Resource
    public Map<String,JedisUtil> jedis;
    @Resource
    public Map<String,RedissonUtil> redisson;

    redis.get("name0").connectionTest();
    redis.get("name1").connectionTest();
    redis.get("name2").connectionTest();
    jedis.get("name0").connectionTest();
    jedis.get("name1").connectionTest();
    jedis.get("name2").connectionTest();
    redisson.get("name0").connectionTest();
    redisson.get("name1").connectionTest();
    redisson.get("name2").connectionTest();
    
```