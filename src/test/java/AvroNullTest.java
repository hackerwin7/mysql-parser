import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import com.github.hackerwin7.mysql.parser.protocol.avro.EventEntryAvro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 15-4-7.
 */
public class AvroNullTest {
    public static void main(String[] args) throws Exception {
//        JdwData data = new JdwData();
//        data.setDb("xxx");
//        data.setTab("111");
//        data.setErr("");
//        data.setSch("xxx");
//        data.setOpt("231");
//        data.setCus(new HashMap<CharSequence, CharSequence>());
//        data.setDdl("");
//        data.setMid(1L);
//        data.setSrc(new HashMap<CharSequence, CharSequence>());
//        data.setTs(1000L);
//        Map<CharSequence, CharSequence> ccm = new HashMap<CharSequence, CharSequence>();
//        ccm.put("one", null);
//        ccm.put("two", "2");
//        ccm.put("three", null);
//        ccm.put("four", "4");
//        data.setCur(ccm);
//        byte[] val = AvroCoderUtils.encode(data, JdwData.SCHEMA$);
//        JdwData dd = AvroCoderUtils.decode(val, JdwData.SCHEMA$);
//        Map<CharSequence, CharSequence> cur = dd.getCur();
//        for(Map.Entry<CharSequence, CharSequence> entry : cur.entrySet()) {
//            CharSequence k = entry.getKey();
//            CharSequence v = entry.getValue();
//            System.out.println(k + ":" + v);
//            if(v == null) {
//                System.out.println("null object");
//            }
//            if(v != null && v.equals("")) {
//                System.out.println("null string");
//            }
//        }
        EventEntryAvro avro = new EventEntryAvro();
        avro.setMid(1L);
        avro.setDb("db");
        avro.setSch("sch");
        avro.setTab("tab");
        avro.setOpt("opt");
        avro.setTs(System.currentTimeMillis());
        avro.setDdl("ddl");
        avro.setErr("");
        avro.setCus(new HashMap<CharSequence, CharSequence>());
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
        avro.setCur(ccm);
        avro.setSrc(new HashMap<CharSequence, CharSequence>());

        byte[] value = getBytesFromAvro(avro);

        EventEntryAvro data = getAvroFromBytes(value);

        Map<CharSequence, CharSequence> cur = data.getCur();
        for(Map.Entry<CharSequence, CharSequence> entry : cur.entrySet()) {
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

    private static EventEntryAvro getAvroFromBytes(byte[] value) {
        SpecificDatumReader<EventEntryAvro> reader = new SpecificDatumReader<EventEntryAvro>(EventEntryAvro.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(value,null);
        EventEntryAvro avro = null;
        try {
            avro = reader.read(null,decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return avro;
    }

    private static byte[] getBytesFromAvro(EventEntryAvro avro) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out,null);
        DatumWriter<EventEntryAvro> writer = new SpecificDatumWriter<EventEntryAvro>(EventEntryAvro.getClassSchema());
        try {
            writer.write(avro,encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] value = out.toByteArray();
        return value;
    }
}
