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
package org.lisapark.koctopus.processors.source;

import com.fasterxml.uuid.Generators;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.lisapark.koctopus.core.Output;
import org.lisapark.koctopus.core.Persistable;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Attribute;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.event.EventType;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.runtime.ProcessingRuntime;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lisapark.koctopus.core.ProcessingException;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.repo.utils.GraphUtils;
import org.lisapark.koctopus.core.graph.api.GraphVocabulary;
import org.lisapark.koctopus.core.source.external.CompiledExternalSource;
import org.lisapark.koctopus.core.source.external.AbstractExternalSource;
import org.lisapark.koctopus.core.transport.Transport;

/**
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class Hash32Generator extends AbstractExternalSource {

    static final Logger LOG = Logger.getLogger(Hash32Generator.class.getName());

    private static final String DEFAULT_NAME = "Hash32Generator";
    private static final String DEFAULT_DESCRIPTION = "Generate source data as 32-bit hash values of random ingtegers.";

    private static final int NUMBER_OF_EVENTS_PARAMETER_ID = 1;

    private static void initAttributeList(Hash32Generator testSource) throws ValidationException {
        testSource.getOutput().addAttribute(Attribute.newAttribute(Integer.class, "Att"));
    }

    public Hash32Generator() {
        super(Generators.timeBasedGenerator().generate());
    }

    public Hash32Generator(UUID id, String name, String description) {
        super(id, name, description);
    }

    private Hash32Generator(UUID id, Hash32Generator copyFromSource) {
        super(id, copyFromSource);
    }

    public Hash32Generator(Hash32Generator copyFromSource) {
        super(copyFromSource);
    }

    public Integer getNumberOfEvents() {
        return getParameter(NUMBER_OF_EVENTS_PARAMETER_ID).getValueAsInteger();
    }

    @Override
    public Hash32Generator copyOf() {
        return new Hash32Generator(this);
    }

    @Override
    public Hash32Generator newInstance() {
        UUID sourceId = Generators.timeBasedGenerator().generate();
        return new Hash32Generator(sourceId, this);
    }

    @Override
    public Hash32Generator newInstance(Gnode gnode) {
        String uuid = gnode.getId() == null ? Generators.timeBasedGenerator().generate().toString() : gnode.getId();
        Hash32Generator testSource = newTemplate(UUID.fromString(uuid));
        GraphUtils.buildSource(testSource, gnode);

        return testSource;
    }

    @Override
    public Hash32Generator newTemplate() {
        UUID sourceId = Generators.timeBasedGenerator().generate();
        return newTemplate(sourceId);
    }

    @Override
    public Hash32Generator newTemplate(UUID sourceId) {
        Hash32Generator testSource = new Hash32Generator(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        testSource.setOutput(Output.outputWithId(1).setName("Output"));
        testSource.addParameter(
                Parameter.integerParameterWithIdAndName(NUMBER_OF_EVENTS_PARAMETER_ID, "Number of Events").
                        description("Number of test events to generate.").
                        defaultValue(100));
        try {
            initAttributeList(testSource);
        } catch (ValidationException ex) {
            LOG.log(Level.SEVERE, ex.getMessage());
        }

        return testSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledTestSource(copyOf());
    }

    @Override
    public <T extends AbstractExternalSource> CompiledExternalSource compile(T source) throws ValidationException {
        return new CompiledTestSource((Hash32Generator) source);
    }

    static class CompiledTestSource implements CompiledExternalSource {

        private final Hash32Generator source;

        /**
         * Running is declared volatile because it may be accessed by different
         * threads
         */
        private volatile boolean running;
        private final long SLIEEP_TIME = 1L;

        public CompiledTestSource(Hash32Generator source) {
            this.source = source;
        }

        @Override
        public Integer startProcessingEvents(Transport runtime) {

            Thread thread = Thread.currentThread();
            runtime.start();
            running = true;
            Integer status = GraphVocabulary.COMPLETE;

            EventType eventType = source.getOutput().getEventType();
            List<Attribute> attributes = eventType.getAttributes();
            int numberEventsCreated = 0;

            while (!thread.isInterrupted() && running && numberEventsCreated < source.getNumberOfEvents()) {
                Event e = createEvent(attributes, numberEventsCreated++);

                runtime.writeEvents(e.getData(), source.getClass().getCanonicalName(), source.getId());

                try {
                    Thread.sleep(SLIEEP_TIME);
                } catch (InterruptedException ex) {
                    status = GraphVocabulary.CANCEL;
                    LOG.log(Level.SEVERE, ex.getMessage());
                }
            }
            return status;
        }

        private Event createEvent(List<Attribute> attributes, int eventNumber) {
            Map<String, Object> attributeData = Maps.newHashMap();

            attributes.forEach((attribute) -> {
                attributeData.put(attribute.getName(), attribute.createSampleData(hash32(eventNumber)));
            });

            return new Event(attributeData);
        }

        @Override
        public void stopProcessingEvents() {
            running = false;
        }

        @Override
        public void startProcessingEvents(ProcessingRuntime runtime) throws ProcessingException {

        }

        private int hash32(int _key) {
            HashCode hashCode = Hashing.murmur3_32().hashInt(_key);
            int key = hashCode.asInt();
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
//        private synchronized void updateBitMapHllLua(final PersistenceClient persistClient, final Client redisClient,
//                String key, boolean update, Multimap<Integer, Integer> map, int bucketNum, long restoreStatus, long index)
//                throws InterruptedException {
//            // Here we are updating heartBeats and restoreStatus
//            //==========================================================
//            if (SimpleUtils.updatePersistence(etcdBase, persistClient, key, index, restoreStatus, update)) {
//                String[] array = multimapToArray(map, bucketNum);
//                redisClient.appendIntArrayBsHllLuaNum(keySha1(key), array);
//            }
//        }
//
//        /**
//         *
//         * @param map
//         * @param bucketNum - total number of buckets (should be power of 2: 2,
//         * 4, 8, 16, 32, 64, 128 and so on)
//         * @return
//         */
//        private synchronized String[] multimapToArray(Multimap<Integer, Integer> map, int bucketNum) {
//            List<String> list = new ArrayList<>();
//            // list(0) => holds bucker size;
//            // =====================================================================
//            list.add(String.valueOf(bucketNum));
//
//            map.keySet().forEach((entry) -> {
//                boolean first = true;
//                Collection<Integer> collection = map.get(entry);
//                for (int value : collection) {
//                    if (first) {
//                        // List(1) => holds bucket Number
//                        list.add(String.valueOf(entry));
//                        // List(2) => holds the number in the bucket
//                        list.add(String.valueOf(map.get(entry).size()));
//                        first = false;
//                    }
//                    list.add(String.valueOf(value));
//                }
//            });
//            String[] array = list.toArray(new String[list.size()]);
//            return array;
//        }
    }
}
