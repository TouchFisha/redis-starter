package com.touchfish.tools.util;

import com.touchfish.tools.interf.IRedisConnection;
import com.touchfish.tools.structure.IPFormat;
import com.touchfish.tools.structure.RedisType;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SentinelServersConfig;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import redis.clients.jedis.*;

import java.util.*;


@Slf4j
public class RedissonUtil implements IRedisConnection<RedissonClient,Config> {
    public static Builder builder() {
        return new Builder(new RedissonUtil());
    }
    private String name;
    private RedisType type = RedisType.NONE;
    private String master = "mymaster";
    private String address;
    private String password;
    private Long timeout = 3000L;
    private Integer database;
    private HostAndPort[] hostAndPorts;
    private RedissonClient redissonClient;
    public RedissonUtil() {}
    public RedissonUtil(String name, RedisType type, String address) {
        new Builder(this).type(type).address(address).build(name);
    }
    public RedissonUtil(String name, RedisType type, String master, String address, String password, int timeout, int database) {
        new Builder(this).type(type).master(master).address(address).password(password).timeout(timeout).database(database).build(name);
    }
    @Override
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
    @Override
    public void refresh() {
        refresh(type);
    }
    /**
     * 重新建立连接
     * @param type 链接redis类型
     */
    @Override
    public void refresh(RedisType type) {
        this.type = RedisType.NONE;
        connect(type);
    }
    /**
     * cluster集群链接
     * @return
     */
    @Override
    public RedissonClient cluster() {
        if (type != RedisType.CLUSTER) {
            close();
            type = RedisType.CLUSTER;
            redissonClient = create(config());
            log.info("Redisson \""+name+"\" "+type+" Client Created.");
        }
        return redissonClient;
    }
    /**
     * 单点独立链接
     * @return
     */
    @Override
    public RedissonClient standalone() {
        if (type != RedisType.STANDALONE) {
            close();
            type = RedisType.STANDALONE;
            redissonClient = create(config());
            log.info("Redisson \""+name+"\" "+type+" Client Created.");
        }
        return redissonClient;
    }
    /**
     * 哨兵集群模式
     * @return
     */
    @Override
    public RedissonClient sentinel() {
        if (type != RedisType.SENTINEL) {
            close();
            type = RedisType.SENTINEL;
            redissonClient = create(config());
            log.info("Redisson \""+name+"\" "+type+" Client Created.");
        }
        return redissonClient;
    }
    /**
     * 根据类型获取连接
     * @param type STANDALONE,CLUSTER,SENTINEL
     * @return
     */
    @Override
    public RedissonClient connect(RedisType type) {
        log.info("Try Access \""+name+"\" "+type+" RedissonClient.");
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
    public RedissonClient create(Config config) {
        return Redisson.create(config);
    }

    /**
     * 获取当前类型的连接
     * @return
     */
    @Override
    public RedissonClient connect() {
        return connect(type);
    }
    @Override
    public boolean connectionTest() {
        boolean res = !connect().isShutdown() && !connect().isShuttingDown();
        if (res) {
            log.info("Redisson \""+name+"\" Successfully Connected.");
        } else {
            log.error("Redisson \""+name+"\" Connect Failed.");
        }
        return res;
    }
    public String name() {
        return name;
    }
    @Override
    public RedisType type() {
        return type;
    }
    @Override
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
    /**
     * 关闭所有连接
     */
    @Override
    public void close() {
        if (redissonClient != null) {
            log.info("Redisson \""+name+"\" "+type+" Client Closed.");
            redissonClient.shutdown();
            redissonClient = null;
        }
    }
    public static String toRedissonAddress(String address) {
        String res = "";
        HostAndPort hostAndPort = AddressUtil.formatAddress(address);
        IPFormat format = AddressUtil.getIPFormat(hostAndPort.toString());
        switch (format) {
            case IPV4:
                res = hostAndPort.toString();
                break;
            case IPV6:
                res = hostAndPort.toString();
//                res = "["+hostAndPort.getHost()+"]:"+hostAndPort.getPort();
//                System.err.println(res);
                break;
        }
        return "redis://"+res;
    }
    @Override
    public Config config() {
        return config(type);
    }
    @Override
    public Config config(RedisType type) {
        Config config = new Config();
        if (type == RedisType.CLUSTER) {
            ClusterServersConfig clusterServersConfig = config.useClusterServers();
            for (HostAndPort hostAndPort : hostAndPorts) {
                clusterServersConfig.addNodeAddress(toRedissonAddress(hostAndPort.toString()));
            }
            if (password != null) {
                clusterServersConfig.setPassword(password);
            }
        } else if (type == RedisType.SENTINEL) {
            SentinelServersConfig sentinelServersConfig = config.useSentinelServers();
            sentinelServersConfig.setMasterName(master);
            sentinelServersConfig.setCheckSentinelsList(false);
            for (HostAndPort hostAndPort : hostAndPorts) {
                sentinelServersConfig.addSentinelAddress(toRedissonAddress(hostAndPort.toString()));
            }
            if (password != null) {
                sentinelServersConfig.setPassword(password);
            }
        } else if (hostAndPorts.length > 0) {
            SingleServerConfig singleServerConfig = config.useSingleServer();
            singleServerConfig.setAddress(toRedissonAddress(hostAndPorts[0].toString()));
            if (password != null) {
                singleServerConfig.setPassword(password);
            }
        }
        return config;
    }
    public static Config config(RedisProperties properties) {
        Config config = new Config();
        if (properties != null) {
            if (properties.getCluster() != null) {
                RedisProperties.Cluster cluster = properties.getCluster();
                ClusterServersConfig clusterServersConfig = config.useClusterServers();
                for (String node : cluster.getNodes()) {
                    clusterServersConfig.addNodeAddress(toRedissonAddress(node));
                }
                if (properties.getPassword() != null) {
                    clusterServersConfig.setPassword(properties.getPassword());
                }
            } else if (properties.getSentinel() != null) {
                RedisProperties.Sentinel sentinel = properties.getSentinel();
                SentinelServersConfig sentinelServersConfig = config.useSentinelServers();
                sentinelServersConfig.setMasterName(sentinel.getMaster());
                for (String node : sentinel.getNodes()) {
                    sentinelServersConfig.addSentinelAddress(toRedissonAddress(node));
                }
                if (properties.getPassword() != null) {
                    sentinelServersConfig.setPassword(properties.getPassword());
                }
            } else if (properties.getHost() != null) {
                SingleServerConfig singleServerConfig = config.useSingleServer();
                singleServerConfig.setAddress(toRedissonAddress(properties.getHost() + ":" + properties.getPort()));
                singleServerConfig.setPassword(properties.getPassword());
            }
        }
        return config;
    }
    public static class Builder {
        RedissonUtil target;
        public Builder() {
            this.target = new RedissonUtil();
        }
        public Builder(RedissonUtil target) {
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
        public RedissonUtil build(String name) {
            if (name != null && !name.isEmpty())
                target.name = name;
            else target.name = "New RedissonUtil " + UUID.randomUUID();
            target.init();
            return target;
        }
        public RedissonUtil build() {
            return build("");
        }
    }
}
