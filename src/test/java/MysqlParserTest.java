import com.github.hackerwin7.mysql.parser.parser.MysqlParser;

import java.io.IOException;

/**
 * Created by hp on 14-9-17.
 */
public class MysqlParserTest {

    public static void main(String[] args) throws IOException{
        MysqlParser mysqlParser = new MysqlParser();
        mysqlParser.mainProc();
    }

}
