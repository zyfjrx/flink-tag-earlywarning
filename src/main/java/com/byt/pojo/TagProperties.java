package com.byt.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @title: mysql配置信息pojo类
 * @author: zhang
 * @date: 2022/6/23 13:33
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TagProperties {
    public Integer id;
    public String tag_name;
    public String tag_topic;
}
