package com.byt.func;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/6/13 9:49
 **/
public interface ToSendMailFunction<T> {
    String getMsg(T input);
}
