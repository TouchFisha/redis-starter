package com.touchfish.tools.interf;

import com.touchfish.tools.structure.RedisType;

public interface IRedisConnection<Connection,ConnectionConfig> extends IStateConnectable<RedisType,Connection,ConnectionConfig> {
    Connection standalone();
    Connection cluster();
    Connection sentinel();
}