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

import java.util.*;
@SuppressWarnings("all")
public class DataRouterCoFlatMap extends RichCoFlatMapFunction<Datapoint, Request, Datapoint> {

    private static final long serialVersionUID = 1L;
    // SourceID (1-5), StreamID(1-10000), Keys(1-1000),

    //UID, parallelism, index,
    //Parallelism //Number, index

    /**
     * A map that holds the parallelism configuration for keyed streams,
     * mapping the number of parallel instances to a tuple containing
     * the count and index for the keyed parallelism scheme.
     *
     * The HashMap has the form of:
     *
     *    Key:  NumOfParallelism --> Tuple2< Number, Index > (int,int)
     *
     */
    private HashMap<Integer, Tuple2<Integer, Integer>> KeyedParallelism = new HashMap<>();


    //Parallelism //Number, index
    /**
     * A map that holds the parallelism configuration for keyed streams,
     * mapping the number of parallel instances to a tuple containing
     * the count and index for the random parallelism scheme.
     *
     * The HashMap has the form of:
     *
     *    Key:  NumOfParallelism --> Tuple2< Number, Index > (int,int)
     */
    private HashMap<Integer, Tuple2<Integer, Integer>> RandomParallelism = new HashMap<>();


    //StreamID //Parallelism, KEYs
    /**
     * A map that hold the parallelism keys for a given data stream. It maps a single
     * streamID to a set of Tuple2s each consisting of degree of parallelism and key (String).
     *
     * The HashMap has the form of:
     *
     *    Key:  StreamID --> ArrayList< Tuple2< Integer, String > >
     */
    private HashMap<String, ArrayList<Tuple2<Integer, String>>> KeysPerStream = new HashMap<>();


    /**
     * Is this actually populated anywhere?
     */
    private HashMap<String, Request> Synopses = new HashMap<>();



    private HashMap<String, RadiusSketch> MapToGrid = new HashMap<>();

    /**
     * Used for keeping track of currently maintained synopses (?)
     */
    private transient ValueState<Tuple1<String>> rs;

    private int pId;

    /**
     * This flatMap1 method handles the arrival of DataPoint objects in our stream
     * @param dataNode The stream element in this case is a DataPoint object
     * @param out The collector to emit resulting elements to, in this case we emit modified DataPoints
     * @throws Exception
     */
    @Override
    public void flatMap1(Datapoint dataNode, Collector<Datapoint> out) throws Exception {

        // Handle the case of RadiusSketch
        if (MapToGrid.containsKey(dataNode.getDataSetkey())) {
            RadiusSketch RS = MapToGrid.get(dataNode.getDataSetkey());
            ArrayList<Datapoint> sketches_to_grid = (ArrayList<Datapoint>) RS.estimate(dataNode.getValues());
            for (Datapoint dp : sketches_to_grid) {
                out.collect(dp);
            }
        }

        if (KeyedParallelism.size() > 0) {
            ArrayList<Tuple2<Integer, String>> tmp = KeysPerStream.get(dataNode.getStreamID());
            if (tmp == null) {
                tmp = new ArrayList<>();
                for (Map.Entry<Integer, Tuple2<Integer, Integer>> entry : KeyedParallelism.entrySet()) {
                    Integer key = entry.getKey();
                    Tuple2<Integer, Integer> v = entry.getValue();
                    tmp.add(new Tuple2<>(key, dataNode.getDataSetkey() + "_" + key + "_KEYED_" + v.f1));
                    v.f1++;
                    if (v.f1 >= key) {
                        //v.f1 = 0;
                        // System.out.println(v.f1);
                        entry.setValue(new Tuple2<>(key, 0));
                    }
                }
                KeysPerStream.put(dataNode.getStreamID(), tmp);
            }
            for (Tuple2<Integer, String> t : tmp) {
                dataNode.setDataSetkey(t.f1);
                out.collect(dataNode);
            }

        }
        if (RandomParallelism.size() > 0) {
            for (Map.Entry<Integer, Tuple2<Integer, Integer>> entry : RandomParallelism.entrySet()) {

                Integer key = entry.getKey();
                Tuple2<Integer, Integer> v = entry.getValue();
                dataNode.setDataSetkey(dataNode.getDataSetkey() + "_" + key + "_RANDOM_" + v.f1);

                out.collect(dataNode);
                v.f1++;
                if (v.f1 == key) {
                    v.f1 = 0;
                    entry.setValue(v);
                }
            }
        }
    }

    /**
     * This flatMap2 method handles the arrival of Request objects in the stream
     * and handles the parallelism.
     * @param request The stream element in this case is a Request object
     * @param out The collector to emit resulting elements to, nothing is emitted in this case
     * @throws Exception
     */
    @Override
    public void flatMap2(Request request, Collector<Datapoint> out) throws Exception {

        if (request.getSynopsisID() == 100) {
            MapToGrid.put(request.getDataSetkey(), new RadiusSketch(request.getUID(), request.getParam()[0], request.getParam()[1], "Partitioner", request.getDataSetkey(), request.getParam()));
            return;
        }


        // The handling for Add requests (Regular/Continuous Add) follows the same logic for this operator
        if (request.getRequestID() == 5)
            request.setRequestID(1);

        // When receiving an Add request in case of parallelism greater than 1
        // generate new dataset keys ready upon which the incoming data tuples will be routed
        if (request.getRequestID() == 1 || request.getRequestID() == 4) {

            // Only allocate partitions when desired num of parallelism is greater than one
            if (request.getNoOfP() > 1) {
                // Add synopsis with Keyed Partitioning (either Regular or Continuous)
                // The following code snippet generates the keys for the corresponding
                // partitions based on the desired number of parallelism
                if (request.getRequestID() == 1) {

                    // Checks if no partition keys are currently generated
                    if (KeyedParallelism.get(request.getNoOfP()) == null) {
                        KeyedParallelism.put(request.getNoOfP(), new Tuple2<>(1, 0));
                        int i = 0;
                        for (ArrayList<Tuple2<Integer, String>> v : KeysPerStream.values()) {
                            v.add(new Tuple2<>(request.getNoOfP(), request.getKey() + "_" + request.getNoOfP() + "_KEYED_" + i));
                            i++;
                            if (i == request.getNoOfP())
                                i = 0;
                        }
                    } else {
                        // Update tha parallelism keys by incrementing by 1 the tuple
                        Tuple2<Integer, Integer> t = KeyedParallelism.get(request.getNoOfP());
                        t.f0 += 1;
                        KeyedParallelism.put(request.getNoOfP(), t);
                    }
                } else if (request.getRequestID() == 4) {

                    if (RandomParallelism.get(request.getNoOfP()) == null) {
                        //Is this correct? Shouldnt it be RandomParallelism?
                        KeyedParallelism.put(request.getNoOfP(), new Tuple2<>(1, 0));
                    } else {
                        Tuple2<Integer, Integer> t = RandomParallelism.get(request.getNoOfP());
                        t.f0++;
                        RandomParallelism.put(request.getNoOfP(), t);
                    }


                }

            }
        // When receiving a Remove request

        } else if (request.getRequestID() == 2) {

            Request re = Synopses.remove("" + request.getUID());
            if (re != null) {

                if (re.getRequestID() == 1) {
                    Tuple2<Integer, Integer> t = KeyedParallelism.get(re.getNoOfP());
                    if (t != null) {
                        //System.out.println("HERE_> " + t.f0);
                        t.f0--;
                        if (t.f0 == 0) {
                            KeyedParallelism.remove(re.getNoOfP());
                            for (ArrayList<Tuple2<Integer, String>> v : KeysPerStream.values()) {
                                for (Tuple2<Integer, String> t2 : v) {
                                    if (t2.f0 == re.getNoOfP()) {
                                        v.remove(t2);
                                        break;
                                    }
                                }
                            }

                        } else {
                            KeyedParallelism.put(re.getNoOfP(), t);
                        }
                    }
                } else if (re.getRequestID() == 4) {
                    Tuple2<Integer, Integer> t = RandomParallelism.get(re.getNoOfP());
                    t.f0--;
                    if (t.f0 == 0) {
                        RandomParallelism.remove(re.getNoOfP());
                    }
                }

                String tmp = "";
                if (Synopses.size() > 0) {
                    for (Request entry : Synopses.values()) {
                        tmp = entry.toSumString().concat(tmp);
                    }

                    Tuple1<String> tmp2 = new Tuple1<>(tmp);
                     // rs.update(tmp2);


                } else {
                   // rs.update(new Tuple1<String>(""));
                }
            }
        }
    }

    public void open(Configuration config) {

        TypeInformation<Tuple1<String>> typeInformation = TypeInformation.of(new TypeHint<Tuple1<String>>() {});
        ValueStateDescriptor descriptor = new ValueStateDescriptor("Synopses", typeInformation);
        descriptor.setQueryable("getSynopses");

        rs = getRuntimeContext().getState(descriptor);
        pId = getRuntimeContext().getIndexOfThisSubtask();

    }

}


