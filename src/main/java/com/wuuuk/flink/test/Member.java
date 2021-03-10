package com.wuuuk.flink.test;

import lombok.Data;
import org.msgpack.annotation.Message;
import java.io.Serializable;



@Message
@Data
public class Member implements Serializable {
    private String resource;
    private String resource_id;
    private String metric;
    private String parent_id;
    private Integer value;
}