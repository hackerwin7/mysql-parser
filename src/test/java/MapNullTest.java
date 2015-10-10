import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 15-4-7.
 */
public class MapNullTest {

    public static void main(String[] args) throws Exception {
        Map<CharSequence, CharSequence> ccm = new HashMap<CharSequence, CharSequence>();
        ccm.put("one", null);
        ccm.put("two", "2");
        ccm.put("three", null);
        ccm.put("four", "4");
        for(Map.Entry<CharSequence, CharSequence> entry : ccm.entrySet()) {
            CharSequence k = entry.getKey();
            CharSequence v = entry.getValue();
            System.out.println(k + ":" + v);
            if(v == null) {
                System.out.println("null object");
            }
            if(v != null && v.equals("")) {
                System.out.println("null string");
            }
        }
    }
}
