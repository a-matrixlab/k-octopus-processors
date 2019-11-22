/* 
 * Copyright (C) 2019 Lisa Park, Inc. (www.lisa-park.net)
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
package org.lisapark.koctopus.processors.sink;

import com.fasterxml.uuid.Generators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.lettuce.core.StreamMessage;
import java.util.HashMap;
import java.util.Iterator;
import org.lisapark.koctopus.core.Input;
import org.lisapark.koctopus.core.Persistable;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.sink.SinkContext;
import org.lisapark.koctopus.core.source.Source;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.repo.utils.GraphUtils;
import org.lisapark.koctopus.core.graph.api.GraphVocabulary;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.transport.TransportReference;
import org.lisapark.koctopus.core.sink.external.CompiledExternalSink;
import org.lisapark.koctopus.core.sink.external.AbstractExternalSink;
import org.lisapark.koctopus.core.transport.Transport;

/**
 * @author alexmy
 */
@Persistable
public class ConsoleFromRedis extends AbstractExternalSink {

    private static final String DEFAULT_NAME = "Console for Redis";
    private static final String DEFAULT_DESCRIPTION = "Console Output from Redis";
    private static final String DEFAULT_INPUT = "Input";

    private static final int ATTRIBUTE_LIST_PARAMETER_ID = 1;
    private static final int PAGE_SIZE_PARAMETER_ID = 2;
    private static final String ATTRIBUTE_LIST = "Show Attributes";
    private static final String ATTRIBUTE_LIST_DESCRIPTION
            = "List comma separated attribute names that you would like to show on Console. Empty - will show all attributes.";

    private static final String PAGE_SIZE = "Page size";
    private static final String PAGE_SIZE_DESCRIPTION
            = "Page size description goes here.";

    private static final int INPUT_ID = 0;

    private final Input<Event> input;

    protected Map<String, TransportReference> sourcerefs = new HashMap<>();

    public ConsoleFromRedis() {
        super(Generators.timeBasedGenerator().generate(), DEFAULT_NAME, DEFAULT_DESCRIPTION);
        input = Input.eventInputWithId(INPUT_ID);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private ConsoleFromRedis(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(INPUT_ID);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private ConsoleFromRedis(UUID id, ConsoleFromRedis copyFromNode) {
        super(id, copyFromNode.getName(), copyFromNode.getDescription());
        input = copyFromNode.getInput().copyOf();
    }

    private ConsoleFromRedis(ConsoleFromRedis copyFromNode) {
        super(copyFromNode.getId(), copyFromNode.getName(), copyFromNode.getDescription());
        this.input = copyFromNode.input.copyOf();
    }

    @SuppressWarnings("unchecked")
    public void setAttributeList(String attributeList) throws ValidationException {
        getParameter(ATTRIBUTE_LIST_PARAMETER_ID).setValue(attributeList);
    }

    public String getAttributeList() {
        return getParameter(ATTRIBUTE_LIST_PARAMETER_ID).getValueAsString();
    }

    @SuppressWarnings("unchecked")
    public void setPageSize(Integer pageSize) throws ValidationException {
        getParameter(PAGE_SIZE_PARAMETER_ID).setValue(pageSize);
    }

    public Integer getPageSize() {
        return getParameter(PAGE_SIZE_PARAMETER_ID).getValueAsInteger();
    }

    @Override
    public List<? extends Input> getInputs() {
        return ImmutableList.of(input);
    }

    public Input getInput() {
        return input;
    }

    @Override
    public boolean isConnectedTo(Source source) {

        return input.isConnectedTo(source);
    }

    @Override
    public void disconnect(Source source) {
        if (input.isConnectedTo(source)) {
            input.clearSource();
        }
    }

    @Override
    public ConsoleFromRedis newInstance() {
        return new ConsoleFromRedis(Generators.timeBasedGenerator().generate(), this);
    }

    @Override
    public ConsoleFromRedis newInstance(Gnode gnode) {
        String uuid = gnode.getId() == null ? Generators.timeBasedGenerator().generate().toString() : gnode.getId();
        ConsoleFromRedis sink = newTemplate(UUID.fromString(uuid));
        GraphUtils.buildSink(sink, gnode);
        return sink;
    }

    @Override
    public ConsoleFromRedis copyOf() {
        return new ConsoleFromRedis(this);
    }

    @Override
    public ConsoleFromRedis newTemplate() {
        UUID sinkId = Generators.timeBasedGenerator().generate();
        return newTemplate(sinkId);
    }

    @Override
    public ConsoleFromRedis newTemplate(UUID sinkId) {
        ConsoleFromRedis consoleSink = new ConsoleFromRedis(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        consoleSink.addParameter(
                Parameter.stringParameterWithIdAndName(ATTRIBUTE_LIST_PARAMETER_ID, ATTRIBUTE_LIST)
                        .description(ATTRIBUTE_LIST_DESCRIPTION)
                .defaultValue("")
        );
        consoleSink.addParameter(
                Parameter.integerParameterWithIdAndName(PAGE_SIZE_PARAMETER_ID, PAGE_SIZE)
                        .description(PAGE_SIZE_DESCRIPTION).defaultValue(100)
        );

        return consoleSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledConsole(copyOf());
    }

    @Override
    public <T extends AbstractExternalSink> CompiledExternalSink compile(T sink) throws ValidationException {
        return new CompiledConsole((ConsoleFromRedis) sink);
    }

    @Override
    public Map<String, TransportReference> getReferences() {
        return sourcerefs;
    }

    @Override
    public void setReferences(Map<String, TransportReference> sourceref) {
        this.sourcerefs = sourceref;
    }

    static class CompiledConsole extends CompiledExternalSink {

        private final ConsoleFromRedis sink;

        /**
         *
         * @param sink
         */
        protected CompiledConsole(ConsoleFromRedis sink) {
            super(sink);
            this.sink = sink;
        }

        /**
         *
         * @param runtime
         * @param eventsByInputId
         */
        @Override
        public synchronized Integer processEvent(Transport runtime) {

            runtime.start();

            String inputName = sink.getInput().getName();
            String sourceClassName = sink.getReferences().get(inputName).getReferenceClass();
            String sourceId = sink.getReferences().get(inputName).getReferenceId();
            int pageSize = sink.getPageSize();

            String offset = "0";
            Integer status = GraphVocabulary.CANCEL;
            while (true) {
                List<StreamMessage<String, String>> list;
                list = runtime.readEvents(sourceClassName, UUID.fromString(sourceId), offset, pageSize);
                if (list.size() > 0) { // a message was read                    
                    list.forEach(msg -> {
                        if (msg != null) {
                            runtime.getStandardOut().println(msg);
                        } else {
                            runtime.getStandardOut().println("event is null");
                        }
                    });
                    offset = list.get(list.size() - 1).getId();
                    status = GraphVocabulary.BACK_LOG;
                } else {
                    status = GraphVocabulary.COMPLETE;
                    break;
                }
            }
            runtime.shutdown();

            return status;
        }

        /**
         *
         * @param ctx
         * @param eventsByInputId
         */
        @Override
        public void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {
        }
    }

    /**
     *
     * @param map
     * @param attr
     * @param outputString
     * @return
     */
    public StringBuilder extractMap(Map<String, Object> map, String attr, StringBuilder outputString) {
        for (Iterator it = map.entrySet().iterator(); it.hasNext();) {
            outputString.append(", ");
            Entry entry = (Entry) it.next();
            outputString.append(entry.getKey()).append("=").append(entry.getValue());
            if (entry.getValue() instanceof Map) {
                Map _map = Maps.newHashMap((Map) entry.getValue());
                outputString = extractMap(_map, entry.getKey().toString(), outputString);
            }
        }
        return outputString;
    }
}
