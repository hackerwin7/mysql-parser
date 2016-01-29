package com.github.hackerwin7.mysql.parser.protocol.json;

import com.github.hackerwin7.mysql.parser.parser.utils.ParserConfig;
import net.sf.json.JSONObject;

/**
 * Created by hp on 14-11-14.
 */
public class LoadURLJson {

    public static void main(String[] args) {
        ParserConfig configer = new ParserConfig();
        ConfigJson configJson = new ConfigJson("jd-mysql-parser-1");
        JSONObject jRoot = configJson.getJson();
        if(jRoot != null) {
            JSONObject jContent = jRoot.getJSONObject("info").getJSONObject("content");
            configer.setHbaseRootDir(jContent.getString("HbaseRootDir"));
            configer.setHbaseDistributed(jContent.getString("HbaseDistributed"));
            configer.setHbaseZkQuorum(jContent.getString("HbaseZkQuorum"));
            configer.setHbaseZkPort(jContent.getString("HbaseZkPort"));
            configer.setDfsSocketTimeout(jContent.getString("DfsSocketTimeout"));
        }

        System.out.println(configer.getHbaseRootDir()+","+configer.getHbaseDistributed()+"," +
                configer.getHbaseZkQuorum()+","+configer.getHbaseZkPort()+","+configer.getDfsSocketTimeout());

    }

}
