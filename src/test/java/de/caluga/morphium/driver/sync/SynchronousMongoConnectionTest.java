package de.caluga.morphium.driver.sync;

import data.UncachedObject;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.singleconnect.SynchronousMongoConnection;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SynchronousMongoConnectionTest {
    private Logger log = LoggerFactory.getLogger(SynchronousMongoConnectionTest.class);

    @Test
    public void testSyncConnection() throws Exception {
        SynchronousMongoConnection con = new SynchronousMongoConnection();
        con.setHostSeed("localhost:27017");
        con.setSlaveOk(true);

        con.connect();
        log.info("Connected");
        con.delete("testdb", "uncached_object", new LinkedHashMap<>(), true, null, null);
        log.info("Deleted old data");

        ObjectMapperImpl objectMapper = new ObjectMapperImpl();
        for (int i = 0; i < 100; i++) {
            UncachedObject o = new UncachedObject("value", 123 + i);
            con.store("testdb", "uncached_object", Arrays.asList(objectMapper.serialize(o)), null);
        }
        log.info("created test data");
        log.info("running find...");
        List<Map<String, Object>> res = con.find("testdb", "uncached_object", Utils.getMap("counter", 123), null, null, 0, 0, 100, null, null, null);
        assertThat(res.size()).isEqualTo(1);
        assertThat(res.get(0).get("counter")).isEqualTo(123);
        log.info("done.");

        log.info("Updating...");
        Map<String, Object> updateInfo = con.update("testdb", "uncached_object", Utils.getMap("_id", res.get(0).get("_id")), Utils.getMap("$set", Utils.getMap("counter", 9999)), false, false, null, null);
        assertThat(updateInfo.get("nModified")).isEqualTo(1);
        log.info("...done");
        log.info("Re-Reading...");

        res = con.find("testdb", "uncached_object", Utils.getMap("_id", res.get(0).get("_id")), null, null, 0, 0, 100, null, null, null);
        assertThat(res.size()).isEqualTo(1);
        assertThat(res.get(0).get("counter")).isEqualTo(9999);
    }


    @Test
    public void testWatch() throws Exception {
        SynchronousMongoConnection con = new SynchronousMongoConnection();
        con.setHostSeed("localhost:27017");
        con.setSlaveOk(true);
        con.setDefaultBatchSize(5);

        con.connect();
        log.info("Connected");
        ObjectMapperImpl objectMapper = new ObjectMapperImpl();

        UncachedObject o = new UncachedObject("value", 123);
        con.store("testdb", "uncached_object", Arrays.asList(objectMapper.serialize(o)), null);
        new Thread() {
            public void run() {
                try {
                    log.info("Thread is waiting...");
                    Thread.sleep(1000);
                    UncachedObject o = new UncachedObject("value", 123);
                    con.store("testdb", "uncached_object", Arrays.asList(objectMapper.serialize(o)), null);
                    log.info("stored data");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
        con.watch("testdb", "uncached_object", 10000, false, null, new DriverTailableIterationCallback() {
            private int counter = 0;

            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                log.info("got data");
                log.info(Utils.toJsonString(data));
                counter++;
            }

            @Override
            public boolean isContinued() {
                return counter < 100;
            }
        });

        log.info("Watch endet");

        con.close();
    }
}
