package infore.SDE.transformations;

import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Request;
import infore.SDE.synopses.RadiusSketch;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;

public class DataRouterCoFlatMapNew extends RichCoFlatMapFunction<Datapoint, Request, Datapoint> {

    private static final long serialVersionUID = 1L;

    private HashMap<Integer, Tuple2<Integer, Integer>> keyedParallelism = new HashMap<>();
    private HashMap<Integer, Tuple2<Integer, Integer>> randomParallelism = new HashMap<>();
    private HashMap<String, ArrayList<Tuple2<Integer, String>>> keysPerStream = new HashMap<>();
    private HashMap<String, Request> synopses = new HashMap<>();
    private HashMap<String, RadiusSketch> mapToGrid = new HashMap<>();
    private transient ValueState<Tuple1<String>> rs;
    private int pId;

    @Override
    public void flatMap1(Datapoint dataNode, Collector<Datapoint> out) throws Exception {
        // Apply RadiusSketch if available
        if (mapToGrid.containsKey(dataNode.getDataSetkey())) {
            RadiusSketch rs = mapToGrid.get(dataNode.getDataSetkey());
            ArrayList<Datapoint> sketchesToGrid = (ArrayList<Datapoint>) rs.estimate(dataNode.getValues());
            for (Datapoint dp : sketchesToGrid) {
                out.collect(dp);
            }
        }

        // Route based on keyed parallelism
        if (!keyedParallelism.isEmpty()) {
            routeWithKeyedParallelism(dataNode, out);
        }

        // Route based on random parallelism
        if (!randomParallelism.isEmpty()) {
            routeWithRandomParallelism(dataNode, out);
        }
    }

    private void routeWithKeyedParallelism(Datapoint dataNode, Collector<Datapoint> out) {
        ArrayList<Tuple2<Integer, String>> tmp = keysPerStream.get(dataNode.getStreamID());
        if (tmp == null) {
            tmp = new ArrayList<>();
            for (Map.Entry<Integer, Tuple2<Integer, Integer>> entry : keyedParallelism.entrySet()) {
                Integer key = entry.getKey();
                Tuple2<Integer, Integer> v = entry.getValue();
                tmp.add(new Tuple2<>(key, dataNode.getDataSetkey() + "_" + key + "_KEYED_" + v.f1));
                v.f1 = (v.f1 + 1) % key;
                entry.setValue(v);
            }
            keysPerStream.put(dataNode.getStreamID(), tmp);
        }
        for (Tuple2<Integer, String> t : tmp) {
            dataNode.setDataSetkey(t.f1);
            out.collect(dataNode);
        }
    }

    private void routeWithRandomParallelism(Datapoint dataNode, Collector<Datapoint> out) {
        for (Map.Entry<Integer, Tuple2<Integer, Integer>> entry : randomParallelism.entrySet()) {
            Integer key = entry.getKey();
            Tuple2<Integer, Integer> v = entry.getValue();
            dataNode.setDataSetkey(dataNode.getDataSetkey() + "_" + key + "_RANDOM_" + v.f1);
            out.collect(dataNode);
            v.f1 = (v.f1 + 1) % key;
            entry.setValue(v);
        }
    }

    @Override
    public void flatMap2(Request request, Collector<Datapoint> out) throws Exception {
        if (request.getSynopsisID() == 100) {
            // Initialize RadiusSketch
            mapToGrid.put(request.getDataSetkey(), new RadiusSketch(request.getUID(), request.getParam()[0], request.getParam()[1], "Partitioner", request.getDataSetkey(), request.getParam()));
        } else {
            processRequest(request);
        }
    }

    private void processRequest(Request request) throws IOException {
        switch (request.getRequestID()) {
            case 1:
            case 4:
                if (request.getNoOfP() > 1) {
                    updateParallelism(request);
                }
                synopses.put(String.valueOf(request.getUID()), request);
                updateState();
                break;
            case 2:
                removeRequest(request);
                break;
        }
    }

    private void updateParallelism(Request request) {
        if (request.getRequestID() == 1) {
            updateKeyedParallelism(request);
        } else if (request.getRequestID() == 4) {
            updateRandomParallelism(request);
        }
    }

    private void updateKeyedParallelism(Request request) {
        keyedParallelism.compute(request.getNoOfP(), (key, value) -> {
            if (value == null) {
                value = new Tuple2<>(1, 0);
            } else {
                value.f0++;
            }
            int i = 0;
            for (ArrayList<Tuple2<Integer, String>> v : keysPerStream.values()) {
                v.add(new Tuple2<>(request.getNoOfP(), request.getKey() + "_" + request.getNoOfP() + "_KEYED_" + i));
                i = (i + 1) % request.getNoOfP();
            }
            return value;
        });
    }

    private void updateRandomParallelism(Request request) {
        randomParallelism.compute(request.getNoOfP(), (key, value) -> {
            if (value == null) {
                value = new Tuple2<>(1, 0);
            } else {
                value.f0++;
            }
            return value;
        });
    }

    private void updateState() throws IOException {
        StringBuilder stateBuilder = new StringBuilder();
        for (Request entry : synopses.values()) {
            stateBuilder.append(entry.toSumString());
        }
        rs.update(new Tuple1<>(stateBuilder.toString()));
    }

    private void removeRequest(Request request) throws IOException {
        Request removed = synopses.remove(String.valueOf(request.getUID()));
        if (removed != null) {
            decrementParallelism(removed);
            updateState();
        }
    }

    private void decrementParallelism(Request request) {
        if (request.getRequestID() == 1) {
            decrementKeyedParallelism(request);
        } else if (request.getRequestID() == 4) {
            decrementRandomParallelism(request);
        }
    }

    private void decrementKeyedParallelism(Request request) {
        keyedParallelism.computeIfPresent(request.getNoOfP(), (key, value) -> {
            value.f0--;
            if (value.f0 == 0) {
                keyedParallelism.remove(key);
                keysPerStream.values().forEach(list -> list.removeIf(t -> t.f0.equals(key)));
            }
            return value;
        });
    }

    private void decrementRandomParallelism(Request request) {
        randomParallelism.computeIfPresent(request.getNoOfP(), (key, value) -> {
            value.f0--;
            if (value.f0 == 0) {
                randomParallelism.remove(key);
            }
            return value;
        });
    }

    @Override
    public void open(Configuration config) {
        // Initialize ValueState descriptor
        ValueStateDescriptor<Tuple1<String>> descriptor = new ValueStateDescriptor<>("Synopses", TypeInformation.of(new TypeHint<Tuple1<String>>() {}));
        descriptor.setQueryable("getSynopses");
        rs = getRuntimeContext().getState(descriptor);

        // Get parallelism index
        pId = getRuntimeContext().getIndexOfThisSubtask();
    }
}
