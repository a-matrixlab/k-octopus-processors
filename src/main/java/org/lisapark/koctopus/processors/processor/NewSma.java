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
import io.lettuce.core.StreamMessage;
import org.lisapark.koctopus.ProgrammerException;
import org.lisapark.koctopus.core.Input;
import org.lisapark.koctopus.core.Output;
import org.lisapark.koctopus.core.Persistable;
import org.lisapark.koctopus.core.ValidationException;
import org.lisapark.koctopus.core.event.Event;
import org.lisapark.koctopus.core.memory.Memory;
import org.lisapark.koctopus.core.memory.MemoryProvider;
import org.lisapark.koctopus.core.parameter.Constraints;
import org.lisapark.koctopus.core.parameter.Parameter;
import org.lisapark.koctopus.core.runtime.ProcessorContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.lisapark.koctopus.core.graph.Gnode;
import org.lisapark.koctopus.repo.utils.GraphUtils;
import org.lisapark.koctopus.core.graph.NodeAttribute;
import org.lisapark.koctopus.core.graph.api.GraphVocabulary;
import org.lisapark.koctopus.core.memory.heap.HeapCircularBuffer;
import org.lisapark.koctopus.core.processor.CompiledProcessor;
import org.lisapark.koctopus.core.processor.AbstractProcessor;
import org.lisapark.koctopus.core.processor.ProcessorInput;
import org.lisapark.koctopus.core.processor.ProcessorOutput;
import org.lisapark.koctopus.core.transport.TransportReference;
import org.lisapark.koctopus.core.transport.Transport;

/**
 * This {@link AbstractProcessor} is used for computing a Simple Moving Average
 * on a single input and producing an average as the output. A simple moving
 * average is formed by computing the average price of a number over a specific
 * number of periods.
 *
 * For example, most moving averages are based on closing prices. A 5-day simple
 * moving average is the five day sum of closing prices divided by five. As its
 * name implies, a moving average is an average that moves. Old data is dropped
 * as new data comes available. This causes the average to move along the time
 * scale.
 *
 * @author dave sinclair(david.sinclair@lisa-park.com)
 */
@Persistable
public class NewSma extends AbstractProcessor<Double> {

    private static final String DEFAULT_NAME = "New SMA Redis";
    private static final String DEFAULT_DESCRIPTION = "Copy of Simple Moving Average from Redis.";
    private static final String DEFAULT_WINDOW_LENGTH_DESCRIPTION = "Number of data points to consider when calculating the average.";
    private static final String DEFAULT_INPUT_DESCRIPTION = "This is the attribute from the connected source that the"
            + " SMA will be averaging.";
    private static final String DEFAULT_OUTPUT_DESCRIPTION = "This is the name of the output attribute that the"
            + " SMA is producing.";

    /**
     * Sma takes only one parameter, the size of time window. This is the
     * identifier of the parameter.
     */
    private static final int WINDOW_LENGTH_PARAMETER_ID = 2;

    /**
     * Sma takes a single input
     */
    private static final int INPUT_ID = 1;
    private static final int OUTPUT_ID = 1;

    protected Map<String, TransportReference> procrefs = new HashMap<>();

    public NewSma() {
        super(Generators.timeBasedGenerator().generate(), DEFAULT_NAME, DEFAULT_INPUT_DESCRIPTION);
    }

    protected NewSma(UUID id, String name, String description) {
        super(id, name, description);
    }

    protected NewSma(UUID id, NewSma copyFromSma) {
        super(id, copyFromSma);
    }

    protected NewSma(NewSma copyFromSma) {
        super(copyFromSma);
    }

    public int getWindowLength() {
        return getParameter(WINDOW_LENGTH_PARAMETER_ID).getValueAsInteger();
    }

    @SuppressWarnings("unchecked")
    public void setWindowLength(int windowLength) throws ValidationException {
        getParameter(WINDOW_LENGTH_PARAMETER_ID).setValue(windowLength);
    }

    public ProcessorInput getInput() {
        // there is only one input for an Sma
        return getInputs().get(0);
    }

    @Override
    public NewSma copyOf() {
        return new NewSma(this);
    }

    @Override
    public NewSma newInstance() {
        return new NewSma(Generators.timeBasedGenerator().generate(), this);
    }

    @Override
    public NewSma newInstance(Gnode gnode) {
        String uuid = gnode.getId() == null ? Generators.timeBasedGenerator().generate().toString() : gnode.getId();
        NewSma smaRedis = newTemplate(UUID.fromString(uuid));
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
    public NewSma newTemplate() {
        UUID uuid = Generators.timeBasedGenerator().generate();
        return newTemplate(uuid);
    }

    @Override
    public NewSma newTemplate(UUID uuid) {
        NewSma sma = new NewSma(uuid, DEFAULT_NAME, DEFAULT_DESCRIPTION);
        // sma only has window length paramater
        sma.addParameter(
                Parameter.integerParameterWithIdAndName(WINDOW_LENGTH_PARAMETER_ID, "Time window").
                        description(DEFAULT_WINDOW_LENGTH_DESCRIPTION).
                        defaultValue(10).required(true).
                        constraint(Constraints.integerConstraintWithMinimumAndMessage(1, "Time window should be greater than 1."))
        );
        
        // only a single double input
        sma.addInput(
                ProcessorInput.doubleInputWithId(INPUT_ID).name("Input").description(DEFAULT_INPUT_DESCRIPTION)
        );
        // double output
        try {
            sma.setOutput(
                    ProcessorOutput.doubleOutputWithId(OUTPUT_ID).name("SMA").description(DEFAULT_OUTPUT_DESCRIPTION).attributeName("average")
            );
        } catch (ValidationException ex) {
            // this should NOT happen. It means we created the SMA with an invalid attriubte name
            throw new ProgrammerException(ex);
        }
        return sma;
    }

    /**
     * {@link Sma}s need memory to store the prior events that will be used to
     * calculate the average based on. We used a
     * {@link MemoryProvider#createCircularBuffer(int)} to store this data.
     *
     * @param memoryProvider used to create sma's memory
     * @return circular buffer
     */
    @Override
    public Memory<Double> createMemoryForProcessor(MemoryProvider memoryProvider) {
        return memoryProvider.createCircularBuffer(getWindowLength());
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
        NewSma copy = copyOf();
        return new CompiledSma(copy);
    }

    @Override
    public <T extends AbstractProcessor> CompiledProcessor<Double> compile(T processor) throws ValidationException {
        return new CompiledSma((NewSma) processor);
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
    static class CompiledSma extends CompiledProcessor<Double> {

        private final String inputAttributeName;

        private final NewSma sma;

        protected CompiledSma(NewSma sma) {
            super(sma);
            this.sma = sma;
            this.inputAttributeName = sma.getInput().getSourceAttributeName();
        }

        @Override
        public Integer processEvent(Transport runtime) {
            String inputName = sma.getInputs().get(0).getName();
            String outAttName = sma.getOutputAttributeName();
            String sourceClassName = sma.getReferences().get(inputName).getReferenceClass();
            String sourceId = sma.getReferences().get(inputName).getReferenceId();

            Integer status;
            Map<String, NodeAttribute> event = sma.getReferences().get(inputName).getAttributes();
            String inputAttName;
            if (event != null && event.size() == 1) {
                inputAttName = event.keySet().iterator().next();
            } else {
                status = GraphVocabulary.CANCEL;
                return status;
            }

            HeapCircularBuffer<Double> processorMemory = new HeapCircularBuffer<>(sma.getWindowLength());

            runtime.start();
            String offset = "0";
            status = GraphVocabulary.BACK_LOG;
            while (true) {
                // Read messagesfrom the Redis stream
                List<StreamMessage<String, String>> list;
                list = runtime.readEvents(sourceClassName, UUID.fromString(sourceId), offset);
                if (list.size() > 0) { // a message was read                    
                    list.forEach(msg -> {
                        if (msg != null) {
                            String value = msg.getBody().get(inputAttName);
                            Double valueDouble = Double.valueOf(value);
                            processorMemory.add(valueDouble);
                            double total = 0;
                            long numberItems = 0;
                            final Collection<Double> memoryItems = processorMemory.values();
                            for (Double memoryItem : memoryItems) {
                                total += memoryItem;
                                numberItems++;
                            }
                            runtime.getStandardOut().println(msg);
                            // Write calculated sma to the output strim
                            Map<String, String> e = new HashMap<>();
                            Double res = total / numberItems;
                            e.put(outAttName, String.valueOf(res));
                            runtime.writeEvents(e, sma.getClass().getCanonicalName(), sma.getId());
                        } else {
                            runtime.getStandardOut().println("event is null");
                        }
                    });
                    offset = list.get(list.size() - 1).getId();
                } else {
                    status = GraphVocabulary.COMPLETE;
                    runtime.shutdown();
                    break;
                }
            }
            return status;
        }

        @Override
        public Object processEvent(ProcessorContext<Double> ctx, Map<Integer, Event> eventsByInputId) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }
}
