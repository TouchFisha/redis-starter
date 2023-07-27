package com.touchfish.tools.interf;

public interface IRefreshable<T> {
    void refresh();
    void refresh(T t);
}