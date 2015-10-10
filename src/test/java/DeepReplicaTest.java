import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 15-3-13.
 */
public class DeepReplicaTest implements Cloneable {

    public Map<String, Long> maps = new HashMap<String, Long>();


    public Object clone() {
        Object o = null;
        try {
            o = (DeepReplicaTest) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return o;
    }

    public DeepReplicaTest cloneDeep() {
        return (DeepReplicaTest) clone();
    }

    public void clear() {
        if(maps != null) {
            for(Map.Entry<String, Long> entry : maps.entrySet()) {
                maps.put(entry.getKey(), 0L);
            }
        }
    }

    public static void main(String[] args) {
        DeepReplicaTest d1 = new DeepReplicaTest();
        d1.maps.put("one", 1L);
        d1.maps.put("two", 2L);
        d1.maps.put("three", 3L);
        DeepReplicaTest d2 = d1.cloneDeep();
        System.out.println(d2.maps.get("one"));
        d1.clear();
        System.out.println(d2.maps.get("one"));
    }

}
