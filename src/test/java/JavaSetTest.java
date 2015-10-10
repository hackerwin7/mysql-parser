import java.util.HashSet;
import java.util.Set;

/**
 * Created by hp on 15-2-3.
 */
public class JavaSetTest {

    public static void main(String[] args) {
        Set<String> columns = new HashSet<String>();
        columns.add("cake");
        columns.add("create");
        columns.add("delete");
        columns.add("modify");
        columns.add("update");
        columns.add("alter");
        columns.add("select");
        if(columns.contains("cake")) {
            print("cake");
        }
        if(columns.contains("soul")) {
            print("soul");
        }
    }

    private static void print(String str) {
        System.out.print(str);
    }

}
