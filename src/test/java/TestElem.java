import java.util.ArrayList;
import java.util.List;

/**
 * Created by hp on 15-3-13.
 */
public class TestElem {

    public long time;
    public String name;

    public static void main(String[] args) throws Exception {
        TestElem te = null;
        TestElem t1 = new TestElem();
        t1.time = 1;
        TestElem t2 = new TestElem();
        t2.time = 2;
        List<TestElem> tts = new ArrayList<TestElem>();
        tts.add(t1);
        tts.add(t2);
        te = tts.get(tts.size() - 1);
        System.out.println(te.time);
        tts.clear();
        System.out.println(te.time);
    }

}
