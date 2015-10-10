/**
 * Created by hp on 14-12-26.
 */
public class SimpleTest {

    public static void main(String[] args) {
        String ss = "a/u0001b/u0001c/u0001d/u0001";
        String sa = ss.substring(0, ss.lastIndexOf("/u0001"));
        System.out.println(sa);
    }
}
