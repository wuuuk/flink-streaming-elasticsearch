package com.wuuuk.flink.test;


import org.msgpack.MessagePack;
import org.msgpack.template.Templates;

public class MsgpackTest {
    public static void main(String[] args) throws Exception {
        String userInfo = "{\"pId\":9527,\"pName\":\"华安\",\"isMarry\":true}";

        MessagePack messagePack = new MessagePack();
        byte[] raw = messagePack.write(userInfo);
        String std1 = messagePack.read(raw, Templates.TString);
        System.out.println(std1);
    }
}
