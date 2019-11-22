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


import com.fasterxml.uuid.Generators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.lettuce.core.StreamMessage;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Date;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.lisapark.koctopus.ProgrammerException;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.repo.utils.GraphUtils;
import org.lisapark.koctopus.core.graph.api.GraphVocabulary;
import org.lisapark.koctopus.core.lucene.BaseDocLuceneIndex;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.transport.TransportReference;
import org.lisapark.koctopus.core.sink.external.CompiledExternalSink;
import org.lisapark.koctopus.core.sink.external.AbstractExternalSink;
import org.lisapark.koctopus.core.transport.Transport;

/**
 * @author alexmy
 */
@Persistable
public class LuceneBaseIndex extends AbstractExternalSink {

    private static final String DEFAULT_NAME = "LuceneBaseIndex";
    private static final String DEFAULT_DESCRIPTION = "Performs Lucene Indexing of documents.";
    private static final String DEFAULT_INPUT = "Input";

//    private static final int LUCENE_INDEX_ID = 0;
//    private static final int TRANSPORT_PARAMETER_ID = 1;
    private static final int PAGE_SIZE_PARAMETER_ID = 2;
    private static final int FILE_ATTRIBUTE_ID = 3;

    private static final String PAGE_SIZE = "Page size";
    private static final String PAGE_SIZE_DESCRIPTION = "Page size description goes here.";

    private static final int INPUT_ID = 0;

    private final Input<Event> input;

    protected Map<String, TransportReference> sourcerefs = new HashMap<>();

    public LuceneBaseIndex() {
        super(Generators.timeBasedGenerator().generate(), DEFAULT_NAME, DEFAULT_DESCRIPTION);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private LuceneBaseIndex(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(INPUT_ID);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private LuceneBaseIndex(UUID id, LuceneBaseIndex copyFromNode) {
        super(id, copyFromNode.getName(), copyFromNode.getDescription());
        input = copyFromNode.getInput().copyOf();
    }

    private LuceneBaseIndex(LuceneBaseIndex copyFromNode) {
        super(copyFromNode.getId(), copyFromNode.getName(), copyFromNode.getDescription());
        this.input = copyFromNode.input.copyOf();
    }

    @SuppressWarnings("unchecked")
    public void setPageSize(Integer pageSize) throws ValidationException {
        getParameter(PAGE_SIZE_PARAMETER_ID).setValue(pageSize);
    }

    public Integer getPageSize() {
        return getParameter(PAGE_SIZE_PARAMETER_ID).getValueAsInteger();
    }

    @SuppressWarnings("unchecked")
    public void setFileAttributeName(String attrName) throws ValidationException {
        getParameter(FILE_ATTRIBUTE_ID).setValue(attrName);
    }

    public String getFileAttributeName() {
        return getParameter(FILE_ATTRIBUTE_ID).getValueAsString();
    }

    @Override
    public List<? extends Input> getInputs() {
        return ImmutableList.of(input);
    }

    public Input getInput() {
        return getInputs().get(INPUT_ID);
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
    public LuceneBaseIndex newInstance() {
        return new LuceneBaseIndex(Generators.timeBasedGenerator().generate(), this);
    }

    @Override
    public LuceneBaseIndex newInstance(Gnode gnode) {
        String uuid = gnode.getId() == null ? Generators.timeBasedGenerator().generate().toString() : gnode.getId();
        LuceneBaseIndex sink = newTemplate(UUID.fromString(uuid));
        GraphUtils.buildSink(sink, gnode);
        return sink;
    }

    @Override
    public LuceneBaseIndex copyOf() {
        return new LuceneBaseIndex(this);
    }

    @Override
    public  LuceneBaseIndex newTemplate() {
        UUID sinkId = Generators.timeBasedGenerator().generate();
        return newTemplate(sinkId);
    }

    @Override
    public LuceneBaseIndex newTemplate(UUID sinkId) {
        LuceneBaseIndex luceneBaseIndex = new LuceneBaseIndex(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
//        luceneBaseIndex.addParameter(
//                Parameter.stringParameterWithIdAndName(TRANSPORT_PARAMETER_ID, "Redis URL").
//                        description("Redis URL.").
//                        defaultValue("redis://localhost"));
//        luceneBaseIndex.addParameter(Parameter.stringParameterWithIdAndName(LUCENE_INDEX_ID, "Docs Lucene Index")
//                .description("Path to Document Lucene Index Directory.").required(true).defaultValue("")
//        );
        luceneBaseIndex.addParameter(
                Parameter.integerParameterWithIdAndName(PAGE_SIZE_PARAMETER_ID, PAGE_SIZE)
                        .description(PAGE_SIZE_DESCRIPTION).defaultValue(100)
        );
        luceneBaseIndex.addParameter(Parameter.stringParameterWithIdAndName(FILE_ATTRIBUTE_ID, "File Attribute Name")
                .description("Attribute name in the stream that holds file names.").required(true).defaultValue("")
        );
        return luceneBaseIndex;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        return new CompiledConsole(copyOf());
    }

    @Override
    public <T extends AbstractExternalSink> CompiledExternalSink compile(T sink) throws ValidationException {
        return new CompiledConsole((LuceneBaseIndex) sink);
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

        static final Logger LOG = Logger.getLogger(CompiledConsole.class.getName());
        private final LuceneBaseIndex luceneSink;

        protected CompiledConsole(LuceneBaseIndex sink) {
            super(sink);
            this.luceneSink = sink;
        }

        @Override
        public synchronized Integer processEvent(Transport runtime) {

            String attrName = luceneSink.getFileAttributeName();

            runtime.start();

            String inputName = luceneSink.getInput().getName();
            String indexPath = luceneSink.getLuceneIndex();

            String sourceClassName = luceneSink.getReferences().get(inputName).getReferenceClass();
            String sourceId = luceneSink.getReferences().get(inputName).getReferenceId();
            int pageSize = luceneSink.getPageSize();

            String offset = "0";
            Integer status = GraphVocabulary.CANCEL;
            while (true) {
                List<StreamMessage<String, String>> list;
                list = runtime.readEvents(sourceClassName, UUID.fromString(sourceId), offset, pageSize);
                if (list.size() > 0) { // a message was read                    
                    list.forEach((StreamMessage<String, String> msg) -> {
                        if (msg != null) {
                            String file = msg.getBody().get(attrName);
                            if (file != null) {
                                try {
                                    indexDoc(indexPath, file);
                                    runtime.getStandardOut().println(msg);
                                } catch (URISyntaxException | UnsupportedEncodingException ex) {
                                    LOG.log(Level.SEVERE, ex.getMessage());
                                }
                            } else {
                                runtime.getStandardOut().println("file name is null");
                            }
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

        private void indexDoc(String indexPath, String docPath) throws URISyntaxException, UnsupportedEncodingException {

            boolean create = true;

//            final Path _docPath = Paths.get(docPath, StandardCharsets.UTF_8.toString());
           
            Date start = new Date();
            try {
                LOG.log(Level.INFO, "Indexing to directory ''{0}''...", indexPath);

                Directory dir = FSDirectory.open(Paths.get(indexPath));
                Analyzer analyzer = new StandardAnalyzer();
                IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
                if (create) {
                    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                } else {
                    // Add new documents to an existing index:
                    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
                }
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    BaseDocLuceneIndex.indexDoc(writer, docPath, System.currentTimeMillis());
                }
                Date end = new Date();
                LOG.log(Level.INFO, "{0} total milliseconds", end.getTime() - start.getTime());
            } catch (IOException e) {
                LOG.log(Level.SEVERE, " caught a {0}\n with message: {1}", new Object[]{e.getClass(), e.getMessage()});
                throw new ProgrammerException(" caught a IOException\n with message: " + e.getMessage());
            }
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
