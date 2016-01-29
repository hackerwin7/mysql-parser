import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import com.github.hackerwin7.mysql.parser.protocol.json.ConfigJson;

/**
 * Created by hp on 6/4/15.
 */
public class JSONStrGenerater {
    public static void main(String[] args) throws Exception {
        String senseVal = "customerName,address,phone,usermob,email,remark,userremark,orderftel,premark";
        String jobStr = "2102172174092,2102172174094,2102172174096,2102172174098,21021721740100,21021721740102,21021721740104,21021721740106,21021721740108,21021721740110,21021721740112,21021721740114,21021721740116,21021721740118,21021721740120,21021721740122,21021721740124,21021721740126,21021721740128,21021721740130";
        String[] jobs = jobStr.trim().split(",");
        for(String jobId : jobs) {
            JSONObject jog = new JSONObject();
            JSONObject jdatag = new JSONObject();
            JSONArray jag = new JSONArray();
            ConfigJson cj = new ConfigJson(jobId, "release.address");
            JSONObject jo = cj.getJson();
            if (jo != null) {
                if (jo.containsKey("data")) {
                    JSONObject jd = jo.getJSONObject("data");
                    if (jd.containsKey("db_tab_meta")) {
                        JSONArray jf = jd.getJSONArray("db_tab_meta");
                        for (int i = 0; i <= jf.size() - 1; i++) {
                            JSONObject jdata = jf.getJSONObject(i);
                            String dbname = jdata.getString("dbname");
                            String tablename = jdata.getString("tablename");
                            JSONObject jg = new JSONObject();
                            jg.accumulate("dbname", dbname);
                            jg.accumulate("tablename", tablename);
                            jg.accumulate("sensefield", senseVal);
                            jag.add(jg);
                        }
                    }
                }
            }
            jdatag.accumulate("db_tab_meta", jag);
            jog.accumulate("data", jdatag);
            System.out.println("http://172.22.178.85:8080/magpie-conf-service/conf/" + jobId + "_sense");
            System.out.println(jog.toString());
        }
    }
}
