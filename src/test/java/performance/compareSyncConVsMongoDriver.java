package performance;

import data.UncachedObject;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.mongodb.MongoDriver;
import de.caluga.morphium.driver.singleconnect.SynchronousMongoConnection;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class compareSyncConVsMongoDriver {
    private Logger log = LoggerFactory.getLogger(compareSyncConVsMongoDriver.class);

    @Test
    public void compareSyncVsMongo() throws Exception {

        MongoDriver mongod = new MongoDriver();
        mongod.setHostSeed("localhost:27017");
        mongod.setConnectionTimeout(1000);
        mongod.setMaxWaitTime(1000);
        mongod.setReadTimeout(1000);
        mongod.setServerSelectionTimeout(1000);
        mongod.connect();

        SynchronousMongoConnection syc = new SynchronousMongoConnection();
        syc.setHostSeed("localhost:27017");
        syc.connect();

        var drivers = Arrays.asList(mongod, syc, mongod, syc);

        for (MorphiumDriver drv : drivers) {
            log.info("Running with driver: " + drv.getClass().getName());
            ObjectMapperImpl om = new ObjectMapperImpl();
            String db = "testdb";
            String coll = "uncached_object";
            drv.delete(db, coll, new HashMap<>(), true, null, null);

            long start = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                UncachedObject u = new UncachedObject("str" + i, i * i);
                u.setMorphiumId(new MorphiumId());
                drv.store(db, coll, Arrays.asList(om.serialize(u)), null);
            }
            for (int i = 0; i < 100; i++) {
                List<Map<String, Object>> lst = drv.find(db, coll, Utils.getMap("counter", Utils.getMap("$eq", i * i)), null, null, 0, 0, 100, null, null, null);
                assertThat(lst.size()).isEqualTo(1);
            }
            long dur = System.currentTimeMillis() - start;
            log.info(drv.getClass().getName() + ": Running 100 writes and 100 reads took " + dur + "ms");
        }

    }

    @Test
    public void compareSyncVsMongoBulkStore() throws Exception {

        MongoDriver mongod = new MongoDriver();
        mongod.setHostSeed("localhost:27017");
        mongod.setConnectionTimeout(1000);
        mongod.setMaxWaitTime(1000);
        mongod.setReadTimeout(1000);
        mongod.setServerSelectionTimeout(1000);
        mongod.connect();

        SynchronousMongoConnection syc = new SynchronousMongoConnection();
        syc.setHostSeed("localhost:27017");
        syc.connect();

        var drivers = Arrays.asList(mongod, syc, mongod, syc);

        for (MorphiumDriver drv : drivers) {
            log.info("Running with driver: " + drv.getClass().getName());
            ObjectMapperImpl om = new ObjectMapperImpl();
            String db = "testdb";
            String coll = "uncached_object";
            drv.delete(db, coll, new HashMap<>(), true, null, null);

            var start = System.currentTimeMillis();
            List<Map<String, Object>> lst = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                UncachedObject u = new UncachedObject("str" + i, i * i);
                u.setMorphiumId(new MorphiumId());
                lst.add(om.serialize(u));
            }
            drv.store(db, coll, lst, null);
            for (int i = 0; i < 100; i++) {
                lst = drv.find(db, coll, Utils.getMap("counter", Utils.getMap("$eq", i * i)), null, null, 0, 0, 100, null, null, null);
                assertThat(lst.size()).isEqualTo(1);
            }
            long dur = System.currentTimeMillis() - start;
            log.info(drv.getClass().getName() + ": Running 100 writes and 100 reads took " + dur + "ms");
        }

    }

    @Test
    public void compareSyncVsMongoBulkInsert() throws Exception {

        MongoDriver mongod = new MongoDriver();
        mongod.setHostSeed("localhost:27017");
        mongod.setConnectionTimeout(1000);
        mongod.setMaxWaitTime(1000);
        mongod.setReadTimeout(1000);
        mongod.setServerSelectionTimeout(1000);
        mongod.connect();

        SynchronousMongoConnection syc = new SynchronousMongoConnection();
        syc.setHostSeed("localhost:27017");
        syc.connect();

        var drivers = Arrays.asList(mongod, syc, mongod, syc);

        for (MorphiumDriver drv : drivers) {
            log.info("Running with driver: " + drv.getClass().getName());
            ObjectMapperImpl om = new ObjectMapperImpl();
            String db = "testdb";
            String coll = "uncached_object";
            drv.delete(db, coll, new HashMap<>(), true, null, null);

            var start = System.currentTimeMillis();
            List<Map<String, Object>> lst = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                UncachedObject u = new UncachedObject("str" + i, i * i);
                u.setMorphiumId(new MorphiumId());
                lst.add(om.serialize(u));
            }
            drv.insert(db, coll, lst, null);
            for (int i = 0; i < 100; i++) {
                lst = drv.find(db, coll, Utils.getMap("counter", Utils.getMap("$eq", i * i)), null, null, 0, 0, 100, null, null, null);
                assertThat(lst.size()).isEqualTo(1);
            }
            long dur = System.currentTimeMillis() - start;
            log.info(drv.getClass().getName() + ": Running 100 writes and 100 reads took " + dur + "ms");
        }

    }
}
