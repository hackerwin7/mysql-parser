import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import com.github.hackerwin7.mysql.parser.protocol.json.ConfigJson;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 14-12-27.
 */
public class JsonArrTest {

    public static void main(String[] args) throws Exception {
//        Map<String, String> disKey = new HashMap<String, String>();
//        Map<String, String> disType = new HashMap<String, String>();
//        Map<String, String> disTopic = new HashMap<String, String>();
//        ConfigJson fcnf = new ConfigJson("", "kafka.distributed.address");
//        JSONArray froot = fcnf.getJsonArr();
//        //parser json
//        if(froot != null) {
//            for(int i = 0; i <= froot.size() - 1; i++) {
//                JSONObject data = froot.getJSONObject(i);
//                String dbname = data.getString("dbname");
//                String tablename = data.getString("tablename");
//                String mapkey = dbname + "." + tablename;
//                String primarykey = data.getString("primarykey");
//                if(primarykey != null) disKey.put(mapkey, primarykey);
//                String sourceType = data.getString("sourceType");
//                if(sourceType != null) disType.put(mapkey, sourceType);
//                String topic = data.getString("topic");
//                if(topic != null) disTopic.put(mapkey, topic);
//            }
//        }

        Map<String, String> disTopic = new HashMap<String, String>();
        Map<String, String> disKey = new HashMap<String, String>();
        Map<String, String> disType = new HashMap<String, String>();
        ConfigJson jcnf = new ConfigJson("", "online.address");
        JSONObject root = jcnf.getJson();
        //parse the json
        if(root != null) {
            JSONObject data = root.getJSONObject("data");
            //JSONArray jf = data.getJSONArray("db_tab_meta");
            JSONArray jf = JSONArray.fromObject("[\n" +
                    "{\"dbname\":\"jd_data\",\"tablename\":\"test\",\"primarykey\":\"uid\",\"sourceType\":\"02\",\"topic\":\"mysql_aa\"},\n" +
                    "{\"dbname\":\"jd_data\",\"tablename\":\"simple\",\"primarykey\":\"uid\",\"sourceType\":\"02\",\"topic\":\"mysql_bb\"}\n" +
                    "]");
            System.out.println(jf.toString());
            for(int i = 0; i <= jf.size() - 1; i++) {
                JSONObject jdata = jf.getJSONObject(i);
                String dbname = jdata.getString("dbname");
                String tablename = jdata.getString("tablename");
                System.out.println("table :" + tablename);
                String mapkey = dbname + "." + tablename;
                String primarykey = jdata.getString("primarykey");
                System.out.println("key :" + primarykey);
                if(primarykey != null) disKey.put(mapkey, primarykey);
                String sourceType = jdata.getString("sourceType");
                if(sourceType != null) disType.put(mapkey, sourceType);
                String topic = jdata.getString("topic");
                if(topic != null) disTopic.put(mapkey, topic);
            }
        }
    }

}
