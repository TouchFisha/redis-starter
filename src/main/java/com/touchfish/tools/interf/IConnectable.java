package com.touchfish.tools.interf;


import java.io.Closeable;

public interface IConnectable<Connection> extends Initialization, Closeable {
    Connection connect();
    boolean connectionTest();
}