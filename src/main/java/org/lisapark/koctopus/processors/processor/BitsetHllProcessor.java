/* 
 * Copyright (C) 2013 Lisa Park, Inc. (www.lisa-park.net)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.lisapark.koctopus.processors.processor;

import com.fasterxml.uuid.Generators;
import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import org.lisapark.koctopus.core.Input;
import org.lisapark.koctopus.core.Output;
import org.lisapark.koctopus.core.Persistable;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.memory.Memory;
import org.lisapark.koctopus.core.memory.MemoryProvider;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.runtime.ProcessorContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.repo.utils.GraphUtils;
import org.lisapark.koctopus.core.graph.api.GraphVocabulary;
import org.lisapark.koctopus.core.processor.CompiledProcessor;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.processor.ProcessorInput;
import org.lisapark.koctopus.core.processor.ProcessorOutput;
import org.lisapark.koctopus.core.transport.TransportReference;
import org.lisapark.koctopus.core.transport.Transport;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

/**
 *
 * @author alexmy (alexmy@lisa-park.com)
 */
@Persistable
public class BitsetHllProcessor extends AbstractProcessor<Double> {

    static final Logger LOG = Logger.getLogger(BitsetHllProcessor.class.getName());

    private static final String DEFAULT_NAME = "BitsetAndHll";
    private static final String DEFAULT_DESCRIPTION = "Calculates Bitset and HLL from single attribute data stream.";

    protected Map<String, TransportReference> procrefs = new HashMap<>();

    public BitsetHllProcessor() {
        super(Generators.timeBasedGenerator().generate(), DEFAULT_NAME, DEFAULT_DESCRIPTION);
    }

    protected BitsetHllProcessor(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected BitsetHllProcessor(UUID id, BitsetHllProcessor copyFromSma) {
        super(id, copyFromSma);
    }

    protected BitsetHllProcessor(BitsetHllProcessor copyFromSma) {
        super(copyFromSma);
    }

    public String getRedisHost() {
        return getParameter(1).getValueAsString();
    }

    public int getRedisPort() {
        return getParameter(2).getValueAsInteger();
    }

    public String getRecordNumber() {
        return getParameter(3).getValueAsString();
    }

    public int getChunkSize() {
        return getParameter(4).getValueAsInteger();
    }

    public int getBucketNum() {
        return getParameter(5).getValueAsInteger();
    }

    //==========================================================================
    /**
     *
     * @return
     */
    public ProcessorInput getInput() {
        // there is only one input for an Sma
        return getInputs().get(0);
    }

    /**
     *
     * @return
     */
    @Override
    public BitsetHllProcessor copyOf() {
        return new BitsetHllProcessor(this);
    }

    /**
     *
     * @return
     */
    @Override
    public BitsetHllProcessor newInstance() {
        return new BitsetHllProcessor(Generators.timeBasedGenerator().generate(), this);
    }

    /**
     *
     * @param gnode
     * @return
     */
    @Override
    public BitsetHllProcessor newInstance(Gnode gnode) {
        String uuid = gnode.getId() == null ? Generators.timeBasedGenerator().generate().toString() : gnode.getId();
        BitsetHllProcessor smaRedis = newTemplate(UUID.fromString(uuid));
        GraphUtils.buildProcessor(smaRedis, gnode);

        return smaRedis;
    }

    /**
     * Returns a new {@link Sma} processor configured with all the appropriate
     * {@link org.lisapark.koctopus.core.parameter.Parameter}s, {@link Input}s
     * and {@link Output}.
     *
     * @return new {@link Sma}
     */
    @Override
    public BitsetHllProcessor newTemplate() {
        UUID uuid = Generators.timeBasedGenerator().generate();
        return newTemplate(uuid);
    }

    /**
     *
     * @param uuid
     * @return
     */
    @Override
    public BitsetHllProcessor newTemplate(UUID uuid) {
        BitsetHllProcessor bshll = new BitsetHllProcessor(uuid, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        bshll.addParameter(
                Parameter.stringParameterWithIdAndName(1, "Redis Host").
                        description("Redis Host URL").
                        defaultValue("localhost").required(true));

        bshll.addParameter(
                Parameter.integerParameterWithIdAndName(2, "Redis Port").
                        description("Redis Port").
                        defaultValue(6379).required(true)
        );

        bshll.addParameter(
                Parameter.stringParameterWithIdAndName(3, "Record Number").
                        description("Record Number in the source recordset.").
                        defaultValue("0").required(true)
        );

        bshll.addParameter(
                Parameter.integerParameterWithIdAndName(4, "Chunk size").
                        description("Chunk size").
                        defaultValue(1000).required(true)
        );

        bshll.addParameter(
                Parameter.integerParameterWithIdAndName(5, "Bucket Num").
                        description("Bucket Num").
                        defaultValue(128).required(true)
        );

        // only a single double input
        bshll.addInput(
                ProcessorInput.stringInputWithId(1).name("Input").description("")
        );
        try {
            // string output
            bshll.setOutput(
                    ProcessorOutput.stringOutputWithId(1).name("BSHLL").description("").attributeName("BS_HLL")
            );
        } catch (ValidationException ex) {
            LOG.log(Level.SEVERE, ex.getMessage());
        }

        // this should NOT happen. It means we created the SMA with an invalid attriubte name
        return bshll;
    }

    /**
     * @param memoryProvider used to create sma's memory
     * @return circular buffer
     */
    @Override
    public Memory<Double> createMemoryForProcessor(MemoryProvider memoryProvider) {
        return memoryProvider.createCircularBuffer(getRedisPort());
    }

    /**
     * Validates and compile this Sma.Doing so takes a "snapshot" of the
     * {@link #getInputs()} and {@link #output} and returns a
     * {@link CompiledProcessor}.
     *
     * @return CompiledProcessor
     * @throws org.lisapark.koctopus.core.ValidationException
     */
    @Override
    public CompiledProcessor<Double> compile() throws ValidationException {
        validate();
        // we copy all the inputs and output taking a "snapshot" of this processor so we are isolated of changes
        BitsetHllProcessor copy = copyOf();
        return new CompiledBsHll(copy);
    }

    @Override
    public <T extends AbstractProcessor> CompiledProcessor<Double> compile(T processor) throws ValidationException {
        return new CompiledBsHll((BitsetHllProcessor) processor);
    }

    @Override
    public Map<String, TransportReference> getReferences() {
        return procrefs;
    }

    @Override
    public void setReferences(Map<String, TransportReference> procrefs) {
        this.procrefs = procrefs;
    }

    /**
     * This {@link CompiledProcessor} is the actual logic that implements the
     * Simple Moving Average.
     */
    static class CompiledBsHll extends CompiledProcessor<Double> {

        private final String inputAttributeName;

        private final BitsetHllProcessor bshll;

        protected CompiledBsHll(BitsetHllProcessor bshll) {
            super(bshll);
            this.bshll = bshll;
            this.inputAttributeName = bshll.getInput().getSourceAttributeName();
        }

        @Override
        public synchronized Integer processEvent(Transport runtime) {
//            String inputName = bshll.getInputs().get(0).getName();
//            String outAttName = bshll.getOutputAttributeName();
//            String sourceClassName = bshll.getReferences().get(inputName).getReferenceClass();
//            String sourceId = bshll.getReferences().get(inputName).getReferenceId();
//
            Integer status;
//            Map<String, NodeAttribute> event = bshll.getReferences().get(inputName).getAttributes();
//            List<String> inputAttName = new ArrayList<>();
//            if (event != null && event.size() > 0) {
//                for (int i = 0; i < event.size(); i++) {
//                    inputAttName.add(event.keySet().iterator().next());
//                }
//            } else {
//                status = GraphVocabulary.CANCEL;
//                return status;
//            }
//
//            HeapCircularBuffer<Double> processorMemory = new HeapCircularBuffer<>(bshll.getRedisPort());

//            runtime.start();
//            String offset = "0";
//            status = GraphVocabulary.BACK_LOG;
            processBsHll();
            status = GraphVocabulary.COMPLETE;

//            while (true) {
//                // Read messagesfrom the Redis stream
//                List<StreamMessage<String, String>> list;
//                list = runtime.readEvents(sourceClassName, UUID.fromString(sourceId), offset);
//                if (list.size() > 0) { // a message was read                    
//                    list.forEach(msg -> {
//                        if (msg != null) {
//                            String value = msg.getBody().get(inputAttName.get(0));
//                            Double valueDouble = Double.valueOf(value);
//                            processorMemory.add(valueDouble);
//                            double total = 0;
//                            long numberItems = 0;
//                            final Collection<Double> memoryItems = processorMemory.values();
//                            for (Double memoryItem : memoryItems) {
//                                total += memoryItem;
//                                numberItems++;
//                            }
////                            runtime.getStandardOut().println(msg);
//                            Map<String, String> e = new HashMap<>();
//                            Double res = total / numberItems;
//                            e.put(outAttName, String.valueOf(res));
//                            runtime.writeEvents(e, bshll.getClass().getCanonicalName(), bshll.getId());
//                        } else {
//                            runtime.getStandardOut().println("event is null");
//                        }
//                    });
//                    offset = list.get(list.size() - 1).getId();
//                } else {
//                    status = GraphVocabulary.COMPLETE;
//                    runtime.shutdown();
//                    break;
//                }
//            }
            return status;
        }

        private void processBsHll() {
            long start = System.currentTimeMillis();

            String host = bshll.getRedisHost();
            int port = bshll.getRedisPort();

            RClient redisClient = new RClient(host, port);
            try {
                // Set Bitmap calculation parameters
                //================================== 

                long max = Integer.MAX_VALUE + 1L;

                String _key = bshll.getClass().getCanonicalName() + ":" + bshll.getId();
                int i_max = Integer.parseInt(bshll.getRecordNumber());
                int j_max = bshll.getChunkSize();
                int k_max = bshll.getBucketNum();
                int b_size = (int) (max / k_max);
                int j = 0;

                SetMultimap<Integer, Integer> map = HashMultimap.create();

                for (int i = 0; i < i_max; i++) {
                    int n = (int) (Math.random() * max);
                    int b_num = n / b_size;
                    map.put(b_num, n);
                    j++;
                    if (j > j_max) {
                        // Update redis roaring bitmap
                        //==========================================================
                        updateBitMapHllLua(redisClient, _key, map, k_max);
                        map = HashMultimap.create();
                        j = 0;
                    }
                }
                // Completion of the command 
                //     - submit rest of the records to Redis roaring bit map
                //     - submit the state of the command as Finished, so it wont be picked up by monitor
                //=================================================================
                if (!map.isEmpty()) {
                    updateBitMapHllLua(redisClient, _key, map, k_max);
                }
                LOG.info("Client connection closed");
                LOG.log(Level.INFO, "Execution time: {0} sec.", (System.currentTimeMillis() - start) / 1000);
            } catch (InterruptedException ex) {
                LOG.log(Level.SEVERE, ex.getMessage());
            }
        }

        @Override
        public Object processEvent(ProcessorContext<Double> ctx, Map<Integer, Event> eventsByInputId) {
            return null;
        }

        private String keySha1(String _key) {
            HashCode hashCode = Hashing.sha1().newHasher()
                    .putString(_key, Charsets.UTF_8)
                    .hash();
            String key = BaseEncoding.base16().lowerCase().encode(hashCode.asBytes());
            return key;
        }

        /**
         *
         * @param persistClient
         * @param redisClient
         * @param key
         * @param update
         * @param map
         * @param restoreStatus
         * @param index
         * @throws InterruptedException
         */
        private synchronized void updateBitMapHllLua(final RClient redisClient,
                String key, Multimap<Integer, Integer> map, int bucketNum)
                throws InterruptedException {
            // Here we are updating heartBeats and restoreStatus
            //==========================================================
            String[] array = multimapToArray(map, bucketNum);
            redisClient.appendIntArrayBsHllLuaNum(keySha1(key), array);
        }

        /**
         *
         * @param map
         * @param bucketNum - total number of buckets (should be power of 2: 2,
         * 4, 8, 16, 32, 64, 128 and so on)
         * @return
         */
        private synchronized String[] multimapToArray(Multimap<Integer, Integer> map, int bucketNum) {
            List<String> list = new ArrayList<>();
            // list(0) => holds bucker size;
            // =====================================================================
            list.add(String.valueOf(bucketNum));

            map.keySet().forEach((entry) -> {
                boolean first = true;
                Collection<Integer> collection = map.get(entry);
                for (int value : collection) {
                    if (first) {
                        // List(1) => holds bucket Number
                        list.add(String.valueOf(entry));
                        // List(2) => holds the number in the bucket
                        list.add(String.valueOf(map.get(entry).size()));
                        first = false;
                    }
                    list.add(String.valueOf(value));
                }
            });
            String[] array = list.toArray(new String[list.size()]);
            return array;
        }
    }

    static class RClient implements Closeable {

        private final Pool<Jedis> pool;

        public Jedis jedis() {
            return pool.getResource();
        }

        private Connection sendCommand(Jedis conn, Command command, String... args) {
            Connection client = conn.getClient();
            client.sendCommand(command, args);
            return client;
        }

        private Connection sendCommand(Jedis conn, Command command, String name, String[] args) {
            Connection client = conn.getClient();
            client.sendCommand(command, joinParameters(SafeEncoder.encode(name), SafeEncoder.encodeMany(args)));
            return client;
        }

        private Connection sendCommand(Jedis conn, Command command, BitOP op, String... args) {
            Connection client = conn.getClient();
            client.sendCommand(command, joinParameters(op.raw, SafeEncoder.encodeMany(args)));
            return client;
        }

        private Connection sendCommand(Jedis conn, Command command, byte[]... args) {
            Connection client = conn.getClient();
            client.sendCommand(command, args);
            return client;
        }

        private byte[][] joinParameters(byte[] first, byte[][] rest) {
            byte[][] result = new byte[rest.length + 1][];
            result[0] = first;
            System.arraycopy(rest, 0, result, 1, rest.length);
            return result;
        }

        /**
         * Create a new client to ReBloom
         *
         * @param pool Jedis connection pool to be used
         */
        public RClient(Pool<Jedis> pool) {
            this.pool = pool;
        }

        /**
         * Create a new client to ReBloom
         *
         * @param host the redis host
         * @param port the redis port
         * @param timeout connection timeout
         * @param poolSize the poolSize of JedisPool
         */
        public RClient(String host, int port, int timeout, int poolSize) {
            JedisPoolConfig conf = new JedisPoolConfig();
            conf.setMaxTotal(poolSize);
            conf.setTestOnBorrow(false);
            conf.setTestOnReturn(false);
            conf.setTestOnCreate(false);
            conf.setTestWhileIdle(false);
            conf.setMinEvictableIdleTimeMillis(60000);
            conf.setTimeBetweenEvictionRunsMillis(30000);
            conf.setNumTestsPerEvictionRun(-1);
            conf.setFairness(true);
            conf.setMaxWaitMillis(timeout);

            pool = new JedisPool(conf, host, port, timeout);
        }

        /**
         * Create a new client to ReBloom
         *
         * @param host the redis host
         * @param port the redis port
         */
        public RClient(String host, int port) {
            this(host, port, 500, 100);
        }

        // edis Schema Registry commands
        //==========================================================================
        public boolean addSchema(String key, String name, String schema) {
            try (Jedis conn = jedis()) {
                return conn.hset(key, name, schema) > 0L;
            }
        }

        public String getSchema(String key, String name) {
            try (Jedis conn = jedis()) {
                return conn.hget(key, name);
            }
        }

        // Reference list of Roaring Bitmap commands
        //==========================================================================
        /**
         * SETBIT("R.SETBIT"), //(same as SETBIT) GETBIT("R.GETBIT"), //(same as
         * GETBIT) BITOP("R.BITOP"), //(same as BITOP) BITCOUNT("R.BITCOUNT"),
         * //(same as BITCOUNT without start and end parameters)
         * BITPOS("R.BITPOS"), //(same as BITPOS without start and end
         * parameters) SETINTARRAY("R.SETINTARRAY"), //(create a roaring bitmap
         * from an integer array) GETINTARRAY("R.GETINTARRAY"), //(get an
         * integer array from a roaring bitmap) SETBITARRAY("R.SETBITARRAY"),
         * //(create a roaring bitmap from a bit array string)
         * GETBITARRAY("R.GETBITARRAY"), //(get a bit array string from a
         * roaring bitmap) // new commands APPENDINTARRAY("R.APPENDINTARRAY"),
         * //(append integers to a roaring bitmap)
         * RANGEINTARRAY("R.RANGEINTARRAY"), //(get an integer array from a
         * roaring bitmap with start and end, so can implements paging)
         * SETRANGE("R.SETRANGE"), //(set or append integer range to a roaring
         * bitmap) SETFULL("R.SETFULL"), //(fill up a roaring bitmap in integer)
         * STAT("R.STAT"), //(get statistical information of a roaring bitmap)
         * OPTIMIZE("R.OPTIMIZE"), //(optimizeBs a roaring bitmap) // missing
         * command BITFIELD("R.BITFIELD"); // as a work around: you can use
         * BITFIELD command against result of R.GETBITARRAY command
         *
         * optimizing bitmap, supposed to make it easy to work with
         *
         * @param name
         * @return
         */
        public String optimizeBs(String name) {
            try (Jedis conn = jedis()) {
                return sendCommand(conn, Command.OPTIMIZE, name).getBulkReply();
            }
        }

        /**
         * Sets a bit in position = pos to 1
         *
         * @param name The name(key) of the bitmap
         * @param pos The offset of the bit to set = b
         * @param b value to set in position pos
         * @return the original of the bit in position pos in the bitmap.
         */
        public boolean setBit(String name, int pos, int b) {
            try (Jedis conn = jedis()) {
                return sendCommand(conn, Command.SETBIT, name, String.valueOf(pos), String.valueOf(b)).getIntegerReply() != 0;
            }
        }

        /**
         *
         * @param name bitmap key
         * @param pos bit position
         * @return bit value in position pos
         */
        public boolean getBit(String name, int pos) {
            try (Jedis conn = jedis()) {
                return sendCommand(conn, Command.SETBIT, name, String.valueOf(pos)).getIntegerReply() != 0;
            }
        }

        public synchronized String setIntArrayBs(String name, String[] args) {
            try (Jedis conn = jedis()) {
                return sendCommand(conn, Command.SETINTARRAY, name, args).getBulkReply();
            }
        }

        public synchronized String appendIntArrayBs(String name, String[] args) {
            try (Jedis conn = jedis()) {
                return sendCommand(conn, Command.APPENDINTARRAY, name, args).getBulkReply();
            }
        }

        public synchronized boolean addIntArrayHll(String name, String[] args) {
            try (Jedis conn = jedis()) {
                return sendCommand(conn, Command.PFADD, name, args).getIntegerReply() != 0;
            }
        }

        /**
         *
         * @param name
         * @param args
         */
        public synchronized void appendIntArrayBsHllLuaNum(String name, String[] args) {
            try (Jedis conn = jedis()) {
                String script
                        = " local bucketsize = tonumber(ARGV[1])"
                        + " local i = 2"
                        + "     while (i < #ARGV) do"
                        + "         local bucket_num = tonumber(ARGV[i])"
                        + "         i = i + 1"
                        + "         local bucket_size = tonumber(ARGV[i])"
                        + "         i = i + 1"
                        + "         local key = bucket_num .. ':' .. KEYS[1]"
                        + "         redis.call('R.APPENDINTARRAY', key .. ':bs', unpack(ARGV, i, math.min((i + bucket_size - 1), #ARGV)))"
                        + "         redis.call('PFADD', key .. ':hll', unpack(ARGV, i, math.min((i + bucket_size - 1), #ARGV)))"
                        + "         i = i + bucket_size"
                        + "     end";

                List<String> params = new ArrayList<>();
                params.addAll(Arrays.asList(args));

                conn.eval(script, ImmutableList.of(name), params);
            }
        }

        public synchronized void appendIntArrayBsHllLuaString(String name, String[] args) {
            try (Jedis conn = jedis()) {
                String script
                        = " local bucketsize = tonumber(ARGV[1])"
                        + " local i = 2"
                        + "     while (i < #ARGV) do"
                        + "         local bucket_id = ARGV[i]"
                        + "         i = i + 1"
                        + "         local bucket_size = tonumber(ARGV[i])"
                        + "         i = i + 1"
                        + "         local key = bucket_id .. ':' .. KEYS[1]"
                        + "         redis.call('R.APPENDINTARRAY', key .. ':bs', unpack(ARGV, i, math.min((i + bucket_size - 1), #ARGV)))"
                        + "         redis.call('PFADD', key .. ':hll', unpack(ARGV, i, math.min((i + bucket_size - 1), #ARGV)))"
                        + "         i = i + bucket_size"
                        + "     end";

                List<String> params = new ArrayList<>();
                params.addAll(Arrays.asList(args));

                conn.eval(script, ImmutableList.of(name), params);
            }
        }

        /**
         *
         * @param name1
         * @param name2
         * @return
         */
        public boolean bitOpNot(String name1, String name2) {
            try (Jedis conn = jedis()) {
                return sendCommand(conn, Command.BITOP, BitOP.NOT, name1, name2).getIntegerReply() != 0;
            }
        }

        /**
         * Like {@link #add(String, String)}, but allows you to store non-string
         * items
         *
         * @param name Name of the filter
         * @param value Value to add to the filter
         * @return true if the item was not previously in the filter
         */
        public boolean setBit(String name, byte[] value) {
            try (Jedis conn = jedis()) {
                return sendCommand(conn, Command.SETBIT, name.getBytes(), value).getIntegerReply() != 0;
            }
        }

        // General purpose commands
        //==========================================================================
        /**
         * Remove the filter
         *
         * @param name
         * @return true if delete the filter, false is not delete the filter
         */
        public boolean delete(String name) {
            try (Jedis conn = jedis()) {
                return conn.del(name) != 0;
            }
        }

        @Override
        public void close() {
            this.pool.close();
        }
    }

    enum Command implements ProtocolCommand {
        SETBIT("R.SETBIT"), //(same as SETBIT)
        GETBIT("R.GETBIT"), //(same as GETBIT)   
        BITOP("R.BITOP"), //(same as BITOP)
        BITCOUNT("R.BITCOUNT"), //(same as BITCOUNT without start and end parameters)
        BITPOS("R.BITPOS"), //(same as BITPOS without start and end parameters)
        SETINTARRAY("R.SETINTARRAY"), //(create a roaring bitmap from an integer array)
        GETINTARRAY("R.GETINTARRAY"), //(get an integer array from a roaring bitmap)
        SETBITARRAY("R.SETBITARRAY"), //(create a roaring bitmap from a bit array string)
        GETBITARRAY("R.GETBITARRAY"), //(get a bit array string from a roaring bitmap)
        // new commands
        APPENDINTARRAY("R.APPENDINTARRAY"), //(append integers to a roaring bitmap)
        RANGEINTARRAY("R.RANGEINTARRAY"), //(get an integer array from a roaring bitmap with start and end, so can implements paging)
        SETRANGE("R.SETRANGE"), //(set or append integer range to a roaring bitmap)
        SETFULL("R.SETFULL"), //(fill up a roaring bitmap in integer)
        STAT("R.STAT"), //(get statistical information of a roaring bitmap)
        OPTIMIZE("R.OPTIMIZE"), //(optimize a roaring bitmap)
        // missing command
        BITFIELD("R.BITFIELD"), // as a work around: you can use BITFIELD command against result of R.GETBITARRAY command
        PFADD("PFADD");                     // HHL add new entries

        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        @Override
        public byte[] getRaw() {
            return raw;
        }
    }

    enum BitOP {
        AND, OR, XOR, NOT;

        public final byte[] raw;

        private BitOP() {
            this.raw = redis.clients.jedis.util.SafeEncoder.encode(name());
        }
    }
}
