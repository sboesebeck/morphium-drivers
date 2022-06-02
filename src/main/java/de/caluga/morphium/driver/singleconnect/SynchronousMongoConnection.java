package de.caluga.morphium.driver.singleconnect;

import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionPoolListener;
import de.caluga.morphium.Collation;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import java.util.stream.Collectors;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:47
 * <p>
 * connects to one node only!
 */
public class SynchronousMongoConnection extends DriverBase {

    private final Logger log = LoggerFactory.getLogger(SynchronousMongoConnection.class);
    private Socket s;
    private OutputStream out;
    private InputStream in;

    //    private Vector<OpReply> replies = new Vector<>();


    private void reconnect() {
        try {
            if (out != null) {
                out.close();
            }
            if (in != null) {
                in.close();
            }
            if (s != null) {
                s.close();
            }
            connect();
        } catch (Exception e) {
            s = null;
            in = null;
            out = null;
            log.error("Could not reconnect!", e);
        }
    }

    @Override
    public void connect(String replSet) throws MorphiumDriverException {
        try {
            String host = getHostSeed()[0];
            String h[] = host.split(":");
            int port = 27017;
            if (h.length > 1) {
                port = Integer.parseInt(h[1]);
            }
            s = new Socket(h[0], port);
            out = s.getOutputStream();
            in = s.getInputStream();


            try {
                Map<String, Object> result = runCommand("local", Utils.getMap("hello", true));
                //log.info("Got result");
//                if (!result.get("ismaster").equals(true)) {
//                    close();
//                    throw new RuntimeException("Cannot run with secondary connection only!");
//                }
                setReplicaSetName((String) result.get("setName"));
                if (replSet != null && !replSet.equals(getReplicaSetName())) {
                    throw new MorphiumDriverException("Replicaset name is wrong - connected to " + getReplicaSetName() + " should be " + replSet);
                }
                //"maxBsonObjectSize" : 16777216,
                //                "maxMessageSizeBytes" : 48000000,
                //                        "maxWriteBatchSize" : 1000,
                setMaxBsonObjectSize((Integer) result.get("maxBsonObjectSize"));
                setMaxMessageSize((Integer) result.get("maxMessageSizeBytes"));
                setMaxWriteBatchSize((Integer) result.get("maxWriteBatchSize"));

            } catch (MorphiumDriverException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            throw new MorphiumDriverNetworkException("connection failed", e);
        }
    }

    @Override
    protected OpMsg getReply(int waitingFor, int timeout) throws MorphiumDriverException {
        return getReply();
    }

    private synchronized OpMsg getReply() throws MorphiumDriverNetworkException {
        return (OpMsg) WireProtocolMessage.parseFromStream(in);
    }


    @Override
    public void connect() throws MorphiumDriverException {
        connect(null);

    }


    @Override
    public boolean isConnected() {
        return s != null && s.isConnected();
    }

    @Override
    public void close() throws MorphiumDriverException {
        //noinspection EmptyCatchBlock
        try {
            s.close();
            s = null;
            out.close();
            in.close();
        } catch (Exception e) {
        }
    }

    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            Map<String, Object> ret = runCommand("admin", Utils.getMap("replSetGetStatus", 1));
            @SuppressWarnings("unchecked") List<Map<String, Object>> mem = (List) ret.get("members");
            if (mem == null) {
                return null;
            }
            //noinspection unchecked
            mem.stream().filter(d -> d.get("optime") instanceof Map).forEach(d -> d.put("optime", ((Map<String, Map<String, Object>>) d.get("optime")).get("ts")));
            return ret;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return runCommand(db, Utils.getMap("dbstats", 1));
    }

    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> getOps(long threshold) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpMsg q = new OpMsg();
            cmd.put("$db", db);
            q.setMessageId(getNextId());
            q.setFirstDoc(cmd);

            OpMsg rep = null;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                try {
                    rep = waitForReply(db, null, q.getMessageId());
                } catch (MorphiumDriverException e) {
                    e.printStackTrace();
                }
            }
            if (rep == null || rep.getFirstDoc() == null) {
                return null;
            }
            return rep.getFirstDoc();
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

    }

    @Override
    public MorphiumCursor initAggregationIteration(String db, String collection, List<Map<String, Object>> aggregationPipeline, ReadPreference readPreference, Collation collation, int batchSize, Map<String, Object> findMetaData) throws MorphiumDriverException {
        return null;
    }

    @Override
    public MorphiumCursor initIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, Collation collation, Map<String, Object> findMetaData) throws MorphiumDriverException {
        if (sort == null) {
            sort = new HashMap<>();
        }
        OpMsg q = new OpMsg();
        q.setMessageId(getNextId());
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("$db", db);
        doc.put("find", collection);
        if (limit > 0) {
            doc.put("limit", limit);
        }
        doc.put("skip", skip);
        if (!query.isEmpty()) {
            doc.put("filter", query);
        }
        doc.put("sort", sort);
        doc.put("batchSize", batchSize);

        q.setFirstDoc(doc);
        q.setFlags(0);
        q.setResponseTo(0);

        OpMsg reply;
        synchronized (SynchronousMongoConnection.this) {
            sendQuery(q);

            int waitingfor = q.getMessageId();
            reply = getReply();
            if (reply.getResponseTo() != waitingfor) {
                throw new MorphiumDriverNetworkException("Got wrong answser. Request: " + waitingfor + " got answer for " + reply.getResponseTo());
            }

        }

        MorphiumCursor crs = new MorphiumCursor();
        @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
        if (cursor != null && cursor.get("id") != null) {
            crs.setCursorId((Long) cursor.get("id"));
        }

        if (cursor != null) {
            if (cursor.get("firstBatch") != null) {
                //noinspection unchecked
                crs.setBatch((List) cursor.get("firstBatch"));
            } else if (cursor.get("nextBatch") != null) {
                //noinspection unchecked
                crs.setBatch((List) cursor.get("nextBatch"));
            }
        }

        SingleConnectCursor internalCursorData = new SingleConnectCursor(this);
        internalCursorData.setBatchSize(batchSize);
        internalCursorData.setCollection(collection);
        internalCursorData.setDb(db);
        //noinspection unchecked
        crs.setInternalCursorObject(internalCursorData);
        return crs;


    }

    /**
     * watch for changes in all databases
     * this is a synchronous call which blocks until the C
     *
     * @param maxWait
     * @param fullDocumentOnUpdate
     * @param pipeline
     * @param cb
     * @throws MorphiumDriverException
     */
    public void watch(int maxWait, boolean fullDocumentOnUpdate, List<Map<String, Object>> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {

    }

    /**
     * watch for changes in the given database
     *
     * @param db
     * @param maxWait
     * @param fullDocumentOnUpdate
     * @param pipeline
     * @param cb
     * @throws MorphiumDriverException
     */
    @Override
    public void watch(String db, int maxWait, boolean fullDocumentOnUpdate, List<Map<String, Object>> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        OpMsg msg = new OpMsg();
        msg.setMessageId(getNextId());
        Map<String, Object> cmd = Utils.getMap("aggregate", (Object) 1).add("pipeline", pipeline)
                .add("$db", db);
        msg.setFirstDoc(cmd);
        sendQuery(msg);
        OpMsg reply = waitForReply(db, null, msg.getMessageId());


    }

    /**
     * watch for changes in the given db and collection
     *
     * @param db
     * @param collection
     * @param maxWait
     * @param fullDocumentOnUpdate
     * @param pipeline
     * @param cb
     * @throws MorphiumDriverException
     */
    @Override
    public void watch(String db, String collection, int maxWait, boolean fullDocumentOnUpdate, List<Map<String, Object>> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        if (maxWait <= 0) maxWait = getReadTimeout();
        OpMsg startMsg = new OpMsg();
        startMsg.setMessageId(getNextId());
        ArrayList<Map<String, Object>> localPipeline = new ArrayList<>();
        localPipeline.add(Utils.getMap("$changeStream", new HashMap<>()));
        if (pipeline != null && !pipeline.isEmpty()) localPipeline.addAll(pipeline);
        Map<String, Object> cmd = Utils.getMap("aggregate", (Object) collection).add("pipeline", localPipeline)
                .add("cursor", Utils.getMap("batchSize", (Object) 1))  //getDefaultBatchSize()
                .add("$db", db);
        startMsg.setFirstDoc(cmd);
        long start = System.currentTimeMillis();
        sendQuery(startMsg);

        OpMsg msg = startMsg;
        while (true) {
            OpMsg reply = waitForReply(db, collection, msg.getMessageId());
            log.info("got answer for watch!");
            Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
            if (cursor == null) throw new MorphiumDriverException("Could not watch - cursor is null");
            log.debug("CursorID:" + cursor.get("id").toString());
            long cursorId = Long.valueOf(cursor.get("id").toString());

            List<Map<String, Object>> result = (List<Map<String, Object>>) cursor.get("firstBatch");
            if (result == null) {
                result = (List<Map<String, Object>>) cursor.get("nextBatch");
            }
            if (result != null) {
                for (Map<String, Object> o : result) {
                    cb.incomingData(o, System.currentTimeMillis() - start);
                }
            }
            if (!cb.isContinued()) {
                killCursors(db, collection, cursorId);
                break;
            }
            if (cursorId != 0) {
                msg = new OpMsg();
                msg.setMessageId(getNextId());

                Map<String, Object> doc = new LinkedHashMap<>();
                doc.put("getMore", cursorId);
                doc.put("collection", collection);
                doc.put("batchSize", 1);   //getDefaultBatchSize());
                doc.put("maxTimeMS", maxWait);
                doc.put("$db", db);
                msg.setFirstDoc(doc);
                sendQuery(msg);
                log.debug("sent getmore....");
            } else {
                log.debug("Cursor exhausted, restarting");
                msg = startMsg;
                msg.setMessageId(getNextId());
                sendQuery(msg);

            }
        }
    }

    @Override
    public MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException {

        long cursorId = crs.getCursorId();
        SingleConnectCursor internalCursorData = (SingleConnectCursor) crs.getInternalCursorObject();

        if (cursorId == 0) {
            return null;
        }
        OpMsg reply;
        synchronized (SynchronousMongoConnection.this) {
            OpMsg q = new OpMsg();

            q.setFirstDoc(Utils.getMap("getMore", (Object) cursorId)
                    .add("$db", internalCursorData.getDb())
                    .add("collection", internalCursorData.getCollection())
                    .add("batchSize", internalCursorData.getBatchSize()
                    ));
            q.setMessageId(getNextId());
            sendQuery(q);
            reply = getReply();
        }
        crs = new MorphiumCursor();
        //noinspection unchecked
        crs.setInternalCursorObject(internalCursorData);
        @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
        if (cursor == null) {
            //cursor not found
            throw new MorphiumDriverException("Iteration failed! Error: " + reply.getFirstDoc().get("code") + "  Message: " + reply.getFirstDoc().get("errmsg"));
        }
        if (cursor.get("id") != null) {
            crs.setCursorId((Long) cursor.get("id"));
        }
        if (cursor.get("firstBatch") != null) {
            //noinspection unchecked
            crs.setBatch((List) cursor.get("firstBatch"));
        } else if (cursor.get("nextBatch") != null) {
            //noinspection unchecked
            crs.setBatch((List) cursor.get("nextBatch"));
        }

        return crs;
    }

    @Override
    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) {
            return;
        }
        SingleConnectCursor internalCursor = (SingleConnectCursor) crs.getInternalCursorObject();
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") Map<String, Object> m = new LinkedHashMap<>();
        m.put("killCursors", internalCursor.getCollection());
        List<Long> cursors = new ArrayList<>();
        cursors.add(crs.getCursorId());
        m.put("cursors", cursors);

    }

    @Override
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> s, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Collation collation, Map<String, Object> findMetaData) throws MorphiumDriverException {
        if (s == null) {
            s = new HashMap<>();
        }
        final Map<String, Integer> sort = s;
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {

            List<Map<String, Object>> ret;

            OpMsg q = new OpMsg();
            q.setFirstDoc(Utils.getMap("find", (Object) collection)
                    .add("$db", db)
                    .add("filter", query)
                    .add("projection", projection)
                    .add("sort", sort)
                    .add("skip", skip)
                    .add("limit", limit)
                    .add("batchSize", batchSize)
                    .add("collation", collation != null ? collation.toQueryObject() : null
                    ));
            q.setMessageId(getNextId());
            q.setFlags(0);
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);

                int waitingfor = q.getMessageId();
                ret = readBatches(waitingfor, db, collection, batchSize);
            }
            return Utils.getMap("values", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("values");

    }


    private List<Map<String, Object>> readBatches(int waitingfor, String db, String collection, int batchSize) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();

        Map<String, Object> doc;
        synchronized (SynchronousMongoConnection.this) {
            while (true) {
                OpMsg reply = getReply();
                if (reply.getResponseTo() != waitingfor) {
                    log.error("Wrong answer - waiting for " + waitingfor + " but got " + reply.getResponseTo());
                    log.error("Document: " + Utils.toJsonString(reply.getFirstDoc()));
                    continue;
                }
                //                    replies.remove(i);
                @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
                if (cursor == null) {
                    //trying result
                    if (reply.getFirstDoc().get("result") != null) {
                        //noinspection unchecked
                        return (List<Map<String, Object>>) reply.getFirstDoc().get("result");
                    }
                    throw new MorphiumDriverException("did not get any data, cursor == null!");
                }
                if (cursor.get("firstBatch") != null) {
                    //noinspection unchecked
                    ret.addAll((List) cursor.get("firstBatch"));
                } else if (cursor.get("nextBatch") != null) {
                    //noinspection unchecked
                    ret.addAll((List) cursor.get("nextBatch"));
                }
                if (((Long) cursor.get("id")) != 0) {
                    //                        log.info("getting next batch for cursor " + cursor.get("id"));
                    //there is more! Sending getMore!

                    //there is more! Sending getMore!
                    OpMsg q = new OpMsg();
                    q.setFirstDoc(Utils.getMap("getMore", (Object) cursor.get("id"))
                            .add("$db", db)
                            .add("collection", collection)
                            .add("batchSize", batchSize)
                    );
                    q.setMessageId(getNextId());
                    waitingfor = q.getMessageId();
                    sendQuery(q);
                } else {
                    break;
                }
            }
        }
        return ret;
    }


    protected void sendQuery(OpMsg q) throws MorphiumDriverException {
        boolean retry = true;
        long start = System.currentTimeMillis();
        while (retry) {
            try {
                if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                    throw new MorphiumDriverException("Could not send message! Timeout!");
                }
                //q.setFlags(4); //slave ok
                out.write(q.bytes());
                out.flush();
                retry = false;
            } catch (IOException e) {
                log.error("Error sending request - reconnecting", e);
                reconnect();

            }
        }
    }

    @Override
    public OpMsg sendAndWaitForReply(OpMsg q) throws MorphiumDriverException {
        sendQuery(q);
        return getReply(q.getMessageId(), getMaxWaitTime());
    }


    @Override
    public long count(String db, String collection, Map<String, Object> query, Collation collation, ReadPreference rp) throws MorphiumDriverException {
        Map<String, Object> ret = new NetworkCallHelper().doCall(() -> {
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("count", collection);
            doc.put("query", query);
            doc.put("$db", db);
            q.setFirstDoc(doc);
            q.setFlags(0);
            q.setResponseTo(0);

            OpMsg rep;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                rep = waitForReply(db, collection,  q.getMessageId());
            }
            Integer n = (Integer) rep.getFirstDoc().get("n");
            return Utils.getMap("count", n == null ? 0 : n);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        return ((int) ret.get("count"));
    }

    @Override
    public long estimatedDocumentCount(String db, String collection, ReadPreference rp) {
        return 0;
    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            int idx = 0;
            objs.forEach(o -> o.putIfAbsent("_id", new MorphiumId()));

            while (idx < objs.size()) {
                OpMsg op = new OpMsg();
                op.setResponseTo(0);
                op.setMessageId(getNextId());
                HashMap<String, Object> map = new LinkedHashMap<>();
                map.put("insert", collection);

                List<Map<String, Object>> docs = new ArrayList<>();
                for (int i = idx; i < idx + 1000 && i < objs.size(); i++) {
                    docs.add(objs.get(i));
                }
                idx += docs.size();
                map.put("documents", docs);
                map.put("$db", db);
                map.put("ordered", false);
                map.put("writeConcern", new HashMap<String, Object>());
                op.setFirstDoc(map);

                synchronized (SynchronousMongoConnection.this) {
                    sendQuery(op);
                    waitForReply(db, collection,  op.getMessageId());
                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public Map<String, Integer> store(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {

            List<Map<String, Object>> opsLst = new ArrayList<>();
            for (Map<String, Object> o : objs) {
                o.putIfAbsent("_id", new ObjectId());
                Map<String, Object> up = new HashMap<>();
                up.put("q", Utils.getMap("_id", o.get("_id")));
                up.put("u", o);
                up.put("upsert", true);
                up.put("multi", false);
                up.put("collation", null);
                //up.put("arrayFilters",list of arrayfilters)
                //up.put("hint",indexInfo);
                //up.put("c",variablesDocument);
                opsLst.add(up);
            }
            update(db, collection, opsLst, false, null);
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        return null;
    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> ops, boolean multiple, boolean upsert, Collation collation, WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> opsLst = new ArrayList<>();
        Map<String, Object> up = new HashMap<>();
        up.put("q", query);
        up.put("u", ops);
        up.put("upsert", upsert);
        up.put("multi", multiple);
        up.put("collation", collation != null ? collation.toQueryObject() : null);
        //up.put("arrayFilters",list of arrayfilters)
        //up.put("hint",indexInfo);
        //up.put("c",variablesDocument);
        opsLst.add(up);
        return update(db, collection, opsLst, false, wc);
    }

    @Override
    public Map<String, Object> update(String db, String collection, List<Map<String, Object>> updateCommands, boolean ordered, WriteConcern wc) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setResponseTo(0);
            op.setMessageId(getNextId());
            HashMap<String, Object> map = new LinkedHashMap<>();
            map.put("update", collection);
            map.put("updates", updateCommands);
            map.put("ordered", ordered);
            map.put("$db", db);
            op.setFirstDoc(map);
            WriteConcern lwc = wc;
            if (lwc == null) lwc = WriteConcern.getWc(0, false, false, 0);
            map.put("writeConcern", lwc.toMongoWriteConcern().asDocument());
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);
                OpMsg res = waitForReply(db, collection,  op.getMessageId());
                return res.getFirstDoc();
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query,
                                      boolean multiple, Collation collation, WriteConcern wc) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setMessageId(getNextId());

            Map<String, Object> o = new LinkedHashMap<>();
            o.put("delete", collection);
            o.put("ordered", false);
            o.put("$db", db);
            Map<String, Object> wrc = new LinkedHashMap<>();
            wrc.put("w", 1);
            wrc.put("wtimeout", 1000);
            wrc.put("fsync", false);
            wrc.put("j", true);

            o.put("writeConcern", wrc);

            Map<String, Object> q = new LinkedHashMap<>();
            q.put("q", query);
            q.put("limit", 0);
            List<Map<String, Object>> del = new ArrayList<>();
            del.add(q);

            o.put("deletes", del);
            op.setFirstDoc(o);


            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);

                int waitingfor = op.getMessageId();
                //        if (wc == null || wc.getW() == 0) {
                waitForReply(db, collection, waitingfor);
                //        }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private OpMsg waitForReply(String db, String collection, int waitingfor) throws MorphiumDriverException {
        OpMsg reply;
        reply = getReply();
        //                replies.remove(i);
        if (reply.getResponseTo() == waitingfor) {
            if (!reply.getFirstDoc().get("ok").equals(1) && !reply.getFirstDoc().get("ok").equals(1.0)) {
                Object code = reply.getFirstDoc().get("code");
                Object errmsg = reply.getFirstDoc().get("errmsg");
                //                throw new MorphiumDriverException("Operation failed - error: " + code + " - " + errmsg, null, collection, db, query);
                MorphiumDriverException mde = new MorphiumDriverException("Operation failed on " + getHostSeed()[0] + " - error: " + code + " - " + errmsg, null, collection, db, null);
                mde.setMongoCode(code);
                mde.setMongoReason(errmsg);

                throw mde;

            } else {
                //got OK message
                //                        log.info("ok");
            }
        }

        return reply;
    }

    @Override
    public void drop(String db, String collection, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setResponseTo(0);
            op.setMessageId(getNextId());

            HashMap<String, Object> map = new LinkedHashMap<>();
            map.put("drop", collection);
            map.put("$db", db);
            op.setFirstDoc(map);
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);
                try {
                    waitForReply(db, collection,  op.getMessageId());
                } catch (Exception e) {
                    log.warn("Drop failed! " + e.getMessage());
                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public void drop(String db, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setResponseTo(0);
            op.setMessageId(getNextId());

            HashMap<String, Object> map = new LinkedHashMap<>();
            map.put("drop", 1);
            map.put("$db", db);
            op.setFirstDoc(map);
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);
                try {
                    waitForReply(db, null,  op.getMessageId());
                } catch (Exception e) {
                    log.error("Drop failed! " + e.getMessage());
                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public boolean exists(String db) throws MorphiumDriverException {
        //noinspection EmptyCatchBlock
        try {
            getDBStats(db);
            return true;
        } catch (MorphiumDriverException e) {
        }
        return false;
    }

    @Override
    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter, Collation collation, ReadPreference rp) throws MorphiumDriverException {
        Map<String, Object> ret = new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setMessageId(getNextId());

            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("distinct", collection);
            cmd.put("field", field);
            cmd.put("query", filter);
            cmd.put("limit", 1);
            cmd.put("$db", db);
            op.setFirstDoc(cmd);

            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);
                //noinspection EmptyCatchBlock
                try {
                    OpMsg res = waitForReply(db, null,  op.getMessageId());
                    log.error("Need to implement distinct");
                } catch (Exception e) {

                }
            }

            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        //noinspection unchecked
        return (List<Object>) ret.get("result");
    }

    @Override
    public boolean exists(String db, String collection) throws MorphiumDriverException {
        List<Map<String, Object>> ret = getCollectionInfo(db, collection);
        for (Map<String, Object> c : ret) {
            if (c.get("name").equals(collection)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("listIndexes", 1);
            cmd.put("$db", db);
            cmd.put("collection", collection);
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            q.setFirstDoc(cmd);
            q.setFlags(0);
            q.setResponseTo(0);

            List<Map<String, Object>> ret;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);

                ret = readBatches(q.getMessageId(), db, null, getMaxWriteBatchSize());
            }
            return Utils.getMap("result", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
    }

    @Override
    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        List<Map<String, Object>> ret = getCollectionInfo(db, null);
        return ret.stream().map(c -> (String) c.get("name")).collect(Collectors.toList());
    }

    @Override
    public Map<String, Object> findAndOneAndDelete(String db, String col, Map<String, Object> query, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        OpMsg msg = new OpMsg();
        msg.setMessageId(getNextId());
        Map<String, Object> cmd = Utils.getMap("findAndModify", (Object) col)
                .add("query", query)
                .add("sort", sort)
                .add("collation", collation != null ? collation.toQueryObject() : null)
                .add("new", true)
                .add("$db", db);
        msg.setFirstDoc(cmd);
        msg.setFlags(0);
        msg.setResponseTo(0);

        OpMsg reply = sendAndWaitForReply(msg);

        return reply.getFirstDoc();
    }

    @Override
    public Map<String, Object> findAndOneAndUpdate(String db, String col, Map<String, Object> query, Map<String, Object> update, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> findAndOneAndReplace(String db, String col, Map<String, Object> query, Map<String, Object> replacement, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        return null;
    }

    private List<Map<String, Object>> getCollectionInfo(String db, String collection) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("listCollections", 1);
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            if (collection != null) {
                cmd.put("filter", Utils.getMap("name", collection));
            }
            cmd.put("$db", db);
            q.setFirstDoc(cmd);
            q.setFlags(0);
            q.setResponseTo(0);

            List<Map<String, Object>> ret;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);

                ret = readBatches(q.getMessageId(), db, null, getMaxWriteBatchSize());
            }
            return Utils.getMap("result", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
    }


    @Override
    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, Collation collation, ReadPreference readPreference) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("aggregate", collection);
            doc.put("$db", db);
            doc.put("pipeline", pipeline);
            doc.put("explain", explain);
            doc.put("allowDiskUse", allowDiskUse);
            doc.put("cursor", Utils.getMap("batchSize", getDefaultBatchSize()));

            q.setFirstDoc(doc);

            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                List<Map<String, Object>> lst = readBatches(q.getMessageId(), db, collection, getMaxWriteBatchSize());
                return Utils.getMap("result", lst);
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
    }

    @Override
    public int getServerSelectionTimeout() {
        return 0;
    }

    @Override
    public void setServerSelectionTimeout(int serverSelectionTimeout) {

    }


    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        List<Map<String, Object>> lst = getCollectionInfo(db, coll);
        try {
            if (!lst.isEmpty() && lst.get(0).get("name").equals(coll)) {
                Object capped = ((Map) lst.get(0).get("options")).get("capped");
                return capped != null && capped.equals(true);
            }
        } catch (Exception e) {
            log.error("Error", e);
        }
        return false;
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return null;
    }

    @Override
    public void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("createIndexes", collection);
            List<Map<String, Object>> lst = new ArrayList<>();
            Map<String, Object> idx = new HashMap<>();
            idx.put("key", index);
            StringBuilder stringBuilder = new StringBuilder();
            for (String k : index.keySet()) {
                stringBuilder.append(k);
                stringBuilder.append("-");
                stringBuilder.append(index.get(k));
            }

            idx.put("name", "idx_" + stringBuilder.toString());
            if (options != null) {
                idx.putAll(options);
            }
            lst.add(idx);
            cmd.put("indexes", lst);
            runCommand(db, cmd);
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query, Map<String, Object> sorting, Collation collation) throws MorphiumDriverException {
        return null;
    }

    @Override
    public void addCommandListener(CommandListener cmd) {

    }

    @Override
    public void removeCommandListener(CommandListener cmd) {

    }

    @Override
    public void addClusterListener(ClusterListener cl) {

    }

    @Override
    public void removeClusterListener(ClusterListener cl) {

    }

    @Override
    public void addConnectionPoolListener(ConnectionPoolListener cpl) {

    }

    @Override
    public void removeConnectionPoolListener(ConnectionPoolListener cpl) {

    }

    @Override
    public void startTransaction() {

    }

    @Override
    public void commitTransaction() {

    }

    @Override
    public MorphiumTransactionContext getTransactionContext() {
        return null;
    }

    @Override
    public void setTransactionContext(MorphiumTransactionContext ctx) {

    }

    @Override
    public void abortTransaction() {

    }

    @Override
    public SSLContext getSslContext() {
        return null;
    }

    @Override
    public void setSslContext(SSLContext sslContext) {

    }

    @Override
    public boolean isSslInvalidHostNameAllowed() {
        return false;
    }

    @Override
    public void setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {

    }
}
