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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.stream.Stream;
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
public class DocDirSource extends AbstractExternalSource {

    static final Logger LOG = Logger.getLogger(DocDirSource.class.getName());

    private static final String DEFAULT_NAME = "DocsFromDirectory";
    private static final String DEFAULT_DESCRIPTION = "Read documents from specified Directory.";

    private static final int DIR_PATH = 1;
    private static final int FILE_NAME_FILTER = 2;
    private static final int FILE_EXTENTION_FILTER = 3;
    private static final int TRANSPORT_PARAMETER_ID = 4;

    private static void initAttributeList(DocDirSource fileDirSource) throws ValidationException {
        fileDirSource.getOutput().addAttribute(Attribute.newAttribute(String.class, "Att"));
    }

    public DocDirSource() {
        super(Generators.timeBasedGenerator().generate());
    }

    public DocDirSource(UUID id, String name, String description) {
        super(id, name, description);
    }

    private DocDirSource(UUID id, DocDirSource copyFromSource) {
        super(id, copyFromSource);
    }

    public DocDirSource(DocDirSource copyFromSource) {
        super(copyFromSource);
    }

    public String getDirPath() {
        return getParameter(DIR_PATH).getValueAsString();
    }

    public String getFileFilter() {
        return getParameter(FILE_NAME_FILTER).getValueAsString();
    }

    public String getExtFilter() {
        return getParameter(FILE_EXTENTION_FILTER).getValueAsString();
    }

    public String getRedisUrl() {
        return getParameterValueAsString(TRANSPORT_PARAMETER_ID);
    }

    @Override
    public DocDirSource copyOf() {
        return new DocDirSource(this);
    }

    @Override
    public DocDirSource newInstance() {
        UUID sourceId = Generators.timeBasedGenerator().generate();
        return new DocDirSource(sourceId, this);
    }

    @Override
    public DocDirSource newInstance(Gnode gnode) {
        String uuid = gnode.getId() == null ? Generators.timeBasedGenerator().generate().toString() : gnode.getId();
        DocDirSource fileDirSource = newTemplate(UUID.fromString(uuid));
        GraphUtils.buildSource(fileDirSource, gnode);

        return fileDirSource;
    }

    public static DocDirSource newTemplate() {
        UUID sourceId = Generators.timeBasedGenerator().generate();
        return newTemplate(sourceId);
    }

    public static DocDirSource newTemplate(UUID sourceId) {
        DocDirSource dirSource = new DocDirSource(sourceId, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        try {
            dirSource.setOutput(Output.outputWithId(DIR_PATH).setName("Output"));
            dirSource.addParameter(Parameter.stringParameterWithIdAndName(DIR_PATH, "Dir Path").
                    description("Directory path to read files from.").required(true));

            dirSource.addParameter(Parameter.stringParameterWithIdAndName(FILE_NAME_FILTER, "File name filter").
                    description("File name regex.").defaultValue("*").required(true));

            dirSource.addParameter(Parameter.stringParameterWithIdAndName(FILE_EXTENTION_FILTER, "File ext filter").
                    description("File ext regex.").defaultValue("*").required(true));

            dirSource.addParameter(Parameter.stringParameterWithIdAndName(TRANSPORT_PARAMETER_ID, "Redis URL").
                    description("Redis URL.").
                    defaultValue("redis://localhost"));
            initAttributeList(dirSource);

        } catch (ValidationException ex) {
            Logger.getLogger(DocDirSource.class.getName()).log(Level.SEVERE, null, ex);
        }
        return dirSource;
    }

    @Override
    public CompiledExternalSource compile() throws ValidationException {
        return new CompiledDocDirSource(copyOf());
    }

    @Override
    public <T extends AbstractExternalSource> CompiledExternalSource compile(T source) throws ValidationException {
        return new CompiledDocDirSource((DocDirSource) source);
    }

    static class CompiledDocDirSource implements CompiledExternalSource {

        private final DocDirSource source;

        /**
         * Running is declared volatile because it may be access my different
         * threads
         */
        private volatile boolean running;

        public CompiledDocDirSource(DocDirSource source) {
            this.source = source;
        }

        @Override
        public Integer startProcessingEvents(Transport runtime) {

            runtime.start();
            running = true;
            Integer status = GraphVocabulary.COMPLETE;

            EventType eventType = source.getOutput().getEventType();
            List<Attribute> attributes = eventType.getAttributes();

            String filepattern = createRegexFromGlob(source.getFileFilter());
            String extpattern = createRegexFromGlob(source.getExtFilter());

            try (Stream<Path> paths = Files.walk(Paths.get(source.getDirPath()))) {
                paths.filter(Files::isRegularFile)
                        .forEach((file) -> {
                            String fileName = file.getFileName().toString();
                            String[] split = fileName.split("\\.");
                            if (split.length == 2 && split[0].matches(filepattern) && split[1].matches(extpattern)
                                    || split.length < 2 && fileName.matches(filepattern)) {
                                String _fileName = file.toAbsolutePath().toString();
                                write(attributes, _fileName, runtime);
                            }
                        });
            } catch (Exception e) {
                status = GraphVocabulary.CANCEL;

            }
            return status;
        }

        private void write(List<Attribute> attributes, String fileName, Transport runtime) {
            Event e = createEvent(attributes, fileName);
            runtime.writeEvents(e.getData(), source.getClass().getCanonicalName(), source.getId());
        }

        private Event createEvent(List<Attribute> attributes, String fileName) {
            Map<String, Object> attributeData = Maps.newHashMap();

            attributes.forEach((attribute) -> {
                attributeData.put(attribute.getName(), fileName);
            });

            return new Event(attributeData);
        }

        private String createRegexFromGlob(String glob) {
            StringBuilder out = new StringBuilder("^");
            for (int i = 0; i < glob.length(); ++i) {
                final char c = glob.charAt(i);
                switch (c) {
                    case '*':
                        out.append(".*");
                        break;
                    case '?':
                        out.append('.');
                        break;
                    case '.':
                        out.append("\\.");
                        break;
                    case '\\':
                        out.append("\\\\");
                        break;
                    default:
                        out.append(c);
                }
            }
            out.append('$');
            return out.toString();
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
