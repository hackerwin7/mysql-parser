import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 15-3-18.
 */
public class MapCharsequenceTest {

    public static void main(String[] args) throws Exception {
        String s = "afs_service_id";
        Map<CharSequence, CharSequence> maps = new HashMap<CharSequence, CharSequence>();
        maps.put("afs_service_id", "1");
        maps.put("ssh", "2");
        if(maps.containsKey(s)) {
            System.out.print("yes");
        } else {
            System.out.print("no");
        }
    }
}
