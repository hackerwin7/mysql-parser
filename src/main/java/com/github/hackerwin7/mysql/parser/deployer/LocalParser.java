package com.github.hackerwin7.mysql.parser.deployer;

import com.github.hackerwin7.mysql.parser.parser.HandlerKafkaZkLocalPerformance;
import org.apache.log4j.Logger;

/**
 * Created by hp on 15-3-2.
 */
public class LocalParser {

    private static Logger logger = Logger.getLogger(LocalParser.class);
    private static boolean running = true;

    public static void main(String[] args) throws Exception {
        while (true) {
            try {
                final HandlerKafkaZkLocalPerformance handler = new HandlerKafkaZkLocalPerformance();
                handler.prepare("mysql-tracker-json");
                while(running) {
                    handler.run();
                }
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        try {
                            running = false;
                            handler.close("mysql-tracker-json");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (Throwable e) {
                logger.info(e.getMessage(), e);
                Thread.sleep(3000);
            }
        }
    }
}
