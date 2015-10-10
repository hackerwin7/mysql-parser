import protocol.json.ConfigJson;
import net.sf.json.JSONObject;
import parser.utils.ParserConfig;

/**
 * Created by hp on 14-11-14.
 */
public class ParserJsonTest {

    public static void main(String[] args) {
        ParserConfig configer = new ParserConfig();
        ConfigJson configJson = new ConfigJson("jd-mysql-parser-1");
        JSONObject jRoot = configJson.getJson();
        if(jRoot != null) {
            JSONObject jContent = jRoot.getJSONObject("info").getJSONObject("content");
            configer.setHbaseRootDir(jContent.getString("HbaseRootDir"));
            configer.setHbaseDistributed(jContent.getString("HbaseDistributed"));
            configer.setHbaseZkQuorum(jContent.getString("HbaseZKQuorum"));
            configer.setHbaseZkPort(jContent.getString("HbaseZKPort"));
            configer.setDfsSocketTimeout(jContent.getString("DfsSocketTimeout"));
        }

        System.out.println(configer.getHbaseRootDir()+","+configer.getHbaseDistributed()+"," +
                configer.getHbaseZkQuorum()+","+configer.getHbaseZkPort()+","+configer.getDfsSocketTimeout());

    }

}
