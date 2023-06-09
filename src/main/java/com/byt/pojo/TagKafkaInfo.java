package com.byt.pojo;

import com.byt.utils.TimeUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * @title: kafka数据封装pojo class
 * @author: zhang
 * @date: 2022/6/22 19:19
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TagKafkaInfo {
    private String name;
    private String time;
    private BigDecimal value;
    private String topic;
    private String strValue;
    private Long timestamp;


    public Long getTimestamp() {
        if (this.timestamp == null && this.time != null) {
            timestamp = TimeUtil.getStartTime(this.time);
        }
        return timestamp;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TagKafkaInfo that = (TagKafkaInfo) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(topic, that.topic) ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, topic);
    }

    @Override
    public String toString() {
        return "TagKafkaInfo{" +
                "name='" + name + '\'' +
                ", time='" + time + '\'' +
                ", value=" + value +
                ", topic='" + topic + '\'' +
                ", strValue='" + strValue + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
