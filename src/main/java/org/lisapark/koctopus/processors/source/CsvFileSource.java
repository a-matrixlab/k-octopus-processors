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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
public class CsvFileSource extends AbstractExternalSource {

    static final Logger LOG = Logger.getLogger(CsvFileSource.class.getName());

    private static final String DEFAULT_NAME = "Csv File data source";
    private static final String DEFAULT_DESCRIPTION = "Reads data from a flat csv file and sends lines as stream messages to the Redis.";

    public CsvFileSource() {
        super(Generators.timeBasedGenerator().generate());
    }

    public CsvFileSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private CsvFileSource(UUID id, CsvFileSource copyFromSource) {
        super(id, copyFromSource);
    }

    public CsvFileSource(CsvFileSource copyFromSource) {
        super(copyFromSource);
    }

    public String getFilePath() {
        return getParameterValueAsString(1);
    }

    public Integer getNumberOfRecords() {
        return getParameter(2).getValueAsInteger();
    }

    public String getRedisUrl() {
        return getParameterValueAsString(3);
    }

    @Override
    public CsvFileSource copyOf() {
        return new CsvFileSource(this);
    }

    @Override
    public CsvFileSource newInstance() {
        UUID sourceId = Generators.timeBasedGenerator().generate();
        return new CsvFileSource(sourceId, this);
    }

    @Override
    public CsvFileSource newInstance(Gnode gnode) {
        String uuid = gnode.getId() == null ? Generators.timeBasedGenerator().generate().toString() : gnode.getId();
        CsvFileSource testSource = newTemplate(UUID.fromString(uuid));
        GraphUtils.buildSource(testSource, gnode);

        return testSource;
    }

    @Override
    public CsvFileSource newTemplate() {
        UUID sourceId = Generators.timeBasedGenerator().generate();
        return newTemplate(sourceId);
    }

    @Override
    public CsvFileSource newTemplate(UUID sourceId) {
        CsvFileSource fileSource = new CsvFileSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        fileSource.setOutput(Output.outputWithId(1).setName("Output"));
        fileSource.addParameter(
                Parameter.stringParameterWithIdAndName(1, "File Path").
                        description("Fully qualified File Path.").
                        defaultValue(""));
        String regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
        fileSource.addParameter(
                Parameter.stringParameterWithIdAndName(2, "Regex").
                        description("Processes commas (and other delimiters) within double quots.").
                        defaultValue(regex));

        fileSource.addParameter(
                Parameter.integerParameterWithIdAndName(3, "Row number").
                        description("Number of rows to read from file.").
                        defaultValue(100));
//        fileSource.addParameter(
//                Parameter.stringParameterWithIdAndName(4, "Redis URL").
//                        description("Redis URL.").
//                        defaultValue("redis://localhost"));

        return fileSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledTestSource(copyOf());
    }

    @Override
    public <T extends AbstractExternalSource> CompiledExternalSource compile(T source) throws ValidationException {
        return new CompiledTestSource((CsvFileSource) source);
    }

    static class CompiledTestSource implements CompiledExternalSource {

        private final CsvFileSource source;

        /**
         * Running is declared volatile because it may be access my different
         * threads
         */
        private volatile boolean running;
        private final long SLIEEP_TIME = 1L;

        public CompiledTestSource(CsvFileSource source) {
            this.source = source;
        }

        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public Integer startProcessingEvents(Transport runtime) {

            Integer status = GraphVocabulary.COMPLETE;
            String pathToCsv = source.getFilePath();
            try {
                Thread thread = Thread.currentThread();
                runtime.start();
                running = true;

                EventType eventType = source.getOutput().getEventType();
                List<Attribute> attributes = eventType.getAttributes();
                int numberEventsCreated = 0;

                File csvfile = new File(pathToCsv);
                if (!csvfile.isFile()) {
                    status = GraphVocabulary.CANCEL;
                    LOG.log(Level.SEVERE, "Wrong file name or file doesn't exist.");

                    return status;
                }

                FileInputStream csvStream = new FileInputStream(csvfile);
                InputStreamReader input = new InputStreamReader(csvStream);
                CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(input);
                List<String> header = csvParser.getHeaderNames();
                for (CSVRecord record : csvParser) {
                    if (!thread.isInterrupted() && running && numberEventsCreated < source.getNumberOfRecords()) {
                        Event e = createEvent(attributes, record);
                        runtime.writeEvents(e.getData(), source.getClass().getCanonicalName(), source.getId());

                        try {
                            Thread.sleep(SLIEEP_TIME);
                        } catch (InterruptedException ex) {
                            status = GraphVocabulary.CANCEL;
                            LOG.log(Level.SEVERE, ex.getMessage());
                        }
                    }
                }
            } catch (IOException ex) {
                Logger.getLogger(CsvFileSource.class.getName()).log(Level.SEVERE, null, ex);
            }
            return status;
        }

        private Event createEvent(List<Attribute> attributes, CSVRecord record) {
            Map<String, Object> attributeData = Maps.newHashMap();

            attributes.forEach((attribute) -> {
                String attrName = attribute.getName();
                attributeData.put(attrName, record.get(attrName));
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
    }
}
