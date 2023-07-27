package com.touchfish.tools.structure;

import lombok.Data;

@Data
public class ExtraRedisProperties {
    public String hostInfo;
    public String password;
    public String connectionTimeout;
    public String database;
    public String master;
    public String maxRedirects;
    public RedisType type;
    public String keySerializer = "StringRedisSerializer";
    public String valueSerializer = "Jackson2JsonRedisSerializer";
    public String hashKeySerializer = "StringRedisSerializer";
    public String hashValueSerializer = "Jackson2JsonRedisSerializer";
}
