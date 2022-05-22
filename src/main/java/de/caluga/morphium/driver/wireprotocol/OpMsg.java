package de.caluga.morphium.driver.wireprotocol;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * see https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst
 * <p>
 * OP_MSG {
 * MsgHeader header;          // standard message header
 * uint32 flagBits;           // message flags
 * Sections[] sections;       // data sections
 * optional<uint32> checksum; // optional CRC-32C checksum
 * }
 * <p>
 * struct MsgHeader {
 * int32   messageLength; // total message size, including this
 * int32   requestID;     // identifier for this message
 * int32   responseTo;    // requestID from the original request
 * //   (used in responses from db)
 * int32   opCode;        // request type - see table below for details
 * }
 * <p>
 * <p>
 * section type 0 (BASIC):
 * byte 0;
 * BSON-Document
 * <p>
 * section type 1 (optimized):
 * byte 1;
 * int32 size
 * CString sequence id;  //reference to insert, id is "documents"/ to update id "updates", to delete id is "deletes"
 * BSON-Documents
 * <p>
 * BSON-Document e.g: {insert: "test_coll", $db: "db", documents: [{_id: 1}]}
 */
public class OpMsg extends WireProtocolMessage {
    public static final int OP_CODE = 2013;


    public static final int CHECKSUM_PRESENT = 1;
    public static final int MORE_TO_COME = 2;
    public static final int EXHAUST_ALLOWED = 65536;


    private List<Map<String, Object>> docs;

    private int flags;

    public void addDoc(Map<String, Object> o) {
        if (docs == null) docs = new ArrayList<>();
        docs.add(o);
    }

    public Map<String, Object> getFirstDoc() {
        if (docs == null || docs.size() == 0) return null;
        return docs.get(0);
    }

    public List<Map<String, Object>> getDocs() {
        return docs;
    }

    public void setDocs(List<Map<String, Object>> docs) {
        this.docs = docs;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }


    @Override
    public void parsePayload(byte[] bytes, int offset) throws IOException {
        flags = readInt(bytes, offset);
        int idx = offset + 4;
        BsonDecoder dec = new BsonDecoder();
        docs = new ArrayList<>();
        while (idx < bytes.length) {
            //reading in sections
            byte section = bytes[idx];
            idx++;
            if (section == 0) {
                Map<String, Object> result = new HashMap<>();
                int l = dec.decodeDocumentIn(result, bytes, idx);
                docs.add(result);
                idx += l;
            }
        }
    }

    public byte[] getPayload() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(flags, out);
        out.write((byte) 0); //section basic
        byte[] d = BsonEncoder.encodeDocument(docs.get(0));
        out.write(d);
        if (docs.size() > 1) {
//            for (int i = 1; i < docs.size(); i++) {
//            }
            throw new RuntimeException("Not supported yet");
        }

        return out.toByteArray();
    }

    @Override
    public int getOpCode() {
        return OpCode.OP_MSG.opCode;
    }

    @Override
    public String toString() {
        return "OpMsg{" +
                "docs=" + Utils.toJsonString(docs.get(0)) +
                ", flags=" + flags +
                '}';
    }
}
