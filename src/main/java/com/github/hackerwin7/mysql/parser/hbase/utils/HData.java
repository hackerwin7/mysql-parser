package com.github.hackerwin7.mysql.parser.hbase.utils;

/**
 * Created by hp on 14-11-25.
 */
public class HData {

    public byte[] rowKey;

    public byte[] rowData;

    public HData(byte[] key, byte[] data) {
        rowData = data;
        rowKey =key;
    }

}
