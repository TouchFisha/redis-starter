package com.touchfish.tools.interf;

import com.touchfish.tools.structure.RedisType;

public interface IStateConnectable<ConnectionType,Connection,ConnectionConfig> extends IConnectable<Connection>, IRefreshable<ConnectionType> {
    ConnectionConfig config();
    ConnectionConfig config(ConnectionType type);
    Connection connect(ConnectionType type);
    Connection create(ConnectionConfig config);
    ConnectionType type();
    void type(ConnectionType type);
}