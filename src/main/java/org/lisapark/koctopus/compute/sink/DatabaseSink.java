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
package org.lisapark.koctopus.compute.sink;

import com.fasterxml.uuid.Generators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import org.lisapark.koctopus.core.AbstractNode;
import org.lisapark.koctopus.core.Input;
import org.lisapark.koctopus.core.ProcessingException;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.runtime.SinkContext;
import org.lisapark.koctopus.core.sink.external.CompiledExternalSink;
import org.lisapark.koctopus.core.sink.external.ExternalSink;
import org.lisapark.koctopus.core.source.Source;
import org.lisapark.koctopus.compute.util.Connections;
import org.lisapark.koctopus.compute.util.DaoUtils;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.core.runtime.redis.StreamReference;
import org.openide.util.Exceptions;
import org.lisapark.koctopus.core.runtime.StreamingRuntime;

/**
 *
 * @author Alex Mylnikov (alexmy@lisa-park.com)
 */
public class DatabaseSink extends AbstractNode implements ExternalSink {

    private final static java.util.logging.Logger LOGGER
            = java.util.logging.Logger.getLogger(DatabaseSink.class.getName());

    private static final String DEFAULT_NAME = "Database";
    private static final String DEFAULT_DESCRIPTION = "Output to Database.";

    private static final int URL_PARAMETER_ID = 1;
    private static final int USER_NAME_PARAMETER_ID = 2;
    private static final int PASSWORD_PARAMETER_ID = 3;
    private static final int DRIVER_PARAMETER_ID = 4;
    private static final int TABLE_PARAMETER_ID = 5;
    private static final int ATTRIBUTE_LIST_PARAMETER_ID = 6;
    private static final int UUID_FIELD_NAME_PARAMETER_ID = 7;

    private static final String ATTRIBUTE_LIST = "Attribute list";
    private static final String ATTRIBUTE_LIST_DESCRIPTION
            = "Comma separated attribute list.";

    private static final String URL = "URL:";
    private static final String USER_NAME = "User name:";
    private static final String PASSWORD = "Passwors:";
    private static final String DRIVER = "Driver class name:";
    private static final String TABLE = "Table name";
    private static final String UUID_FIELD = "Field name for UUID";

    private static final String DEFAULT_INPUT = "Input data";

    private final Input<Event> input;
    protected Map<String, StreamReference> sourceref = new HashMap<>();
    
    private CompiledDatabaseSink compiledExternalSink;

    private DatabaseSink(UUID id, String name, String description) {
        super(id, name, description);
        input = Input.eventInputWithId(1);
        input.setName(DEFAULT_INPUT);
        input.setDescription(DEFAULT_INPUT);
    }

    private DatabaseSink(UUID id, DatabaseSink copyFromNode) {
        super(id, copyFromNode);
        input = copyFromNode.getInput().copyOf();
    }

    public String getUrl() {
        return getParameter(URL_PARAMETER_ID).getValueAsString();
    }

    public String getUserName() {
        return getParameter(USER_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getPassword() {
        return getParameter(PASSWORD_PARAMETER_ID).getValueAsString();
    }

    public String getDriver() {
        return getParameter(DRIVER_PARAMETER_ID).getValueAsString();
    }

    public String getTable() {
        return getParameter(TABLE_PARAMETER_ID).getValueAsString();
    }

    public String getUuidFieldName() {
        return getParameter(UUID_FIELD_NAME_PARAMETER_ID).getValueAsString();
    }

    public String getAttributeList() {
        return getParameter(ATTRIBUTE_LIST_PARAMETER_ID).getValueAsString();
    }

    private DatabaseSink(DatabaseSink copyFromNode) {
        super(copyFromNode);
        this.input = copyFromNode.input.copyOf();
    }

    public Input<Event> getInput() {
        return input;
    }

    @Override
    public List<Input<Event>> getInputs() {
        return ImmutableList.of(input);
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
    public DatabaseSink newInstance() {
        return new DatabaseSink(Generators.timeBasedGenerator().generate(), this);
    }

    @Override
    public DatabaseSink copyOf() {
        return new DatabaseSink(this);
    }

    public static DatabaseSink newTemplate() {
        UUID sinkId = Generators.timeBasedGenerator().generate();

        DatabaseSink databaseSink = new DatabaseSink(sinkId, DEFAULT_NAME, DEFAULT_DESCRIPTION);

        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(URL_PARAMETER_ID, URL)
                .description("Database URL.")
                .defaultValue("jdbc:mysql://173.72.110.131:3306/IPLAST_PROD")
                .required(true));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(USER_NAME_PARAMETER_ID, USER_NAME)
                .description("")
                .defaultValue("root")
                .required(true));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(PASSWORD_PARAMETER_ID, PASSWORD)
                .description("")
                .defaultValue("lisa1234")
                .required(true));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(DRIVER_PARAMETER_ID, DRIVER)
                .description("")
                .defaultValue("com.mysql.jdbc.Driver")
                .required(true));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(TABLE_PARAMETER_ID, TABLE)
                .description("")
                .defaultValue("WORKTABLE_PROD")
                .required(true));
        databaseSink.addParameter(Parameter.stringParameterWithIdAndName(UUID_FIELD_NAME_PARAMETER_ID, UUID_FIELD)
                .description("")
                .defaultValue("RECORD_UUID")
                .required(true));

        databaseSink.addParameter(
                Parameter.stringParameterWithIdAndName(ATTRIBUTE_LIST_PARAMETER_ID, ATTRIBUTE_LIST)
                        .description(ATTRIBUTE_LIST_DESCRIPTION)
                        .defaultValue("RECORD_UUID,DATE,SHOP,SHIFT,MACHINE,PRODUCT,PRODUCT_TYPE,MATERIAL_TYPE,"
                                + "RAW_MATERIAL,TOTAL_MATERIALS,TOTAL_PRODUCTS")
        );

        return databaseSink;
    }

    @Override
    public CompiledExternalSink compile() throws ValidationException {
        compiledExternalSink = new CompiledDatabaseSink(copyOf());
        return compiledExternalSink;
    }

    @Override
    public <T extends ExternalSink> CompiledExternalSink compile(T sink) throws ValidationException {
        compiledExternalSink = new CompiledDatabaseSink(copyOf());
        return compiledExternalSink;
    }

    @Override
    public DatabaseSink newInstance(Gnode gnode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Map<String, StreamReference> getReferences() {
        return sourceref;
    }

    @Override
    public void setReferences(Map<String, StreamReference> sourceref) {
        this.sourceref = sourceref;
    }

    static class CompiledDatabaseSink extends CompiledExternalSink {

        private final DatabaseSink databaseSink;
        private Connection connection = null;

        protected CompiledDatabaseSink(DatabaseSink databaseSink) {
            super(databaseSink);
            this.databaseSink = databaseSink;
        }

        @Override
        public synchronized void processEvent(SinkContext ctx, Map<Integer, Event> eventsByInputId) {

            Event event = eventsByInputId.get(1);

            if (event != null) {
                String attributeList = databaseSink.getAttributeList();
                Map<String, Object> data = extractDataFromEvent(event, attributeList);

                try {
                    connection = getConnection(databaseSink.getDriver(), databaseSink.getUrl(),
                            databaseSink.getUserName(), databaseSink.getPassword());

                    String query = DaoUtils.insertQueryString(data, databaseSink.getTable(), null);

                    LOGGER.log(Level.INFO, "Query: ====> {0}", query);

                    int key = DaoUtils.insert(query, null, connection);

                } catch (SQLException | ProcessingException ex) {
                    Exceptions.printStackTrace(ex);
                }
            } else {
                ctx.getStandardOut().println("event is null");
            }
        }

        private Connection getConnection(String className, String url, String userName, String password) throws ProcessingException {

            try {
                Class.forName(className);
            } catch (ClassNotFoundException e) {
                // this should never happen since the parameter is constrained
                throw new ProcessingException("Could not find JDBC Driver Class " + className, e);
            }

            try {
                if (connection == null) {
                    if (userName == null && password == null) {
                        connection = DriverManager.getConnection(url);
                    } else {
                        connection = DriverManager.getConnection(url, userName, password);
                    }
                }
            } catch (SQLException e) {
                throw new ProcessingException("Could not connect to database. Please check your settings.", e);
            }
            return connection;
        }

        private Map<String, Object> extractDataFromEvent(Event event, String attributeList) {
            Map<String, Object> retMap = Maps.newHashMap();
            Map<String, Object> eventMap = event.getData();

            String[] attList = attributeList.split(",");

            for (String attr : attList) {
                if (attr.equalsIgnoreCase(databaseSink.getUuidFieldName())) {
                    retMap.put(attr, Generators.timeBasedGenerator().generate());
                } else {
                    retMap.put(attr, eventMap.get(attr));
                }
            }

            return retMap;
        }

        @Override
        public Object processEvent(StreamingRuntime runtime) {
            throw new UnsupportedOperationException("Not supported yet.");
            //To change body of generated methods, choose Tools | Templates.
        }
    }

    /**
     * This method is called from ModelRunner to relieve all resources
     */
    @Override
    public void complete() {
        Connections.closeQuietly(compiledExternalSink.connection);
        LOGGER.log(Level.INFO, "Connection: ====> {0}", "Closed!!!");
    }
}
