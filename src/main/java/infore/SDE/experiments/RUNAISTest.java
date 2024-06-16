package infore.SDE.experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.producersForTesting.SendAISTest;
import infore.SDE.sources.kafkaProducerEstimation;
import infore.SDE.sources.KafkaStringConsumer;
import infore.SDE.transformations.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RUNAISTest {


    private static String kafkaDataInputTopic;
    private static String kafkaRequestInputTopic;
    private static String kafkaBrokersList;
    private static String kafkaOutputTopic;

    private static String SOURCE;
    private static int PARALLELISM;
    private static int MULTI;

    /**
     * @param args Program arguments. You have to provide 4 arguments otherwise
     *             DEFAULT values will be used.<br>
     *             <ol>
     *             <li>args[0]={@link #kafkaDataInputTopic} DEFAULT: "Forex")
     *             <li>args[1]={@link #kafkaRequestInputTopic} DEFAULT: "Requests")
     *             <li>args[2]={@link #kafkaBrokersList} (DEFAULT: "localhost:9092")
     *             <li>args[3]={@link #PARALLELISM} Job parallelism (DEFAULT: "4")
     *             <li>args[4]={@link #kafkaOutputTopic} DEFAULT: "OUT")
     *             "O10")
     *             </ol>
     *
     */

    public static void main(String[] args) throws Exception {
        // Initialize Input Parameters
        initializeParameters(args);

        // Populate Kafka Topics with requests and data prior to StreamEnv setup
        if(SOURCE.startsWith("auto")) {
            Thread thread1 = new Thread(() -> {
                (new SendAISTest()).run(kafkaDataInputTopic,kafkaRequestInputTopic, PARALLELISM, kafkaBrokersList);
            });
            thread1.start();
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        KafkaStringConsumer kc = new KafkaStringConsumer(kafkaBrokersList, kafkaDataInputTopic, true);
        KafkaStringConsumer requests = new KafkaStringConsumer(kafkaBrokersList, kafkaRequestInputTopic);
        kafkaProducerEstimation kp = new kafkaProducerEstimation(kafkaBrokersList, kafkaOutputTopic);
        kafkaProducerEstimation pRequest = new kafkaProducerEstimation(kafkaBrokersList, kafkaRequestInputTopic);
        //kafkaProducerEstimation test = new kafkaProducerEstimation(kafkaBrokersList, "testPairs");

        DataStream<String> datastream = env.addSource(kc.getFc());
        DataStream<String> RQ_stream = env.addSource(requests.getFc());

        //map kafka data input to tuple2<int,double>
        DataStream<Datapoint> dataStream = datastream
                .map(new MapFunction<String, Datapoint>() {
                    @Override
                    public Datapoint map(String node) throws IOException {
                        // TODO Auto-generated method stub
                        ObjectMapper objectMapper = new ObjectMapper();
                        Datapoint dp = objectMapper.readValue(node, Datapoint.class);
                        return dp;
                    }
                }).name("DATA_SOURCE");

        //
        DataStream<Request> RQ_Stream = RQ_stream
                .map(new MapFunction<String, Request>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Request map(String node) throws IOException {
                        // TODO Auto-generated method stub
                        //String[] valueTokens = node.replace("\"", "").split(",");
                        //if(valueTokens.length > 6) {
                        ObjectMapper objectMapper = new ObjectMapper();

                        // byte[] jsonData = json.toString().getBytes();
                        Request request = objectMapper.readValue(node, Request.class);
                        return  request;
                    }
                }).name("REQUEST_SOURCE").keyBy((KeySelector<Request, String>) Request::getKey);

        DataStream<Request> SynopsisRequests = RQ_Stream
                .flatMap(new RqRouterFlatMap()).name("REQUEST_ROUTER");


        DataStream<Datapoint> DataStream = dataStream.connect(RQ_Stream)
                .flatMap(new DummydataRouterCoFlatMap(PARALLELISM)).name("DATA_ROUTER")
                .keyBy((KeySelector<Datapoint, String>) Datapoint::getKey);

        DataStream<Datapoint> DataStream2 = DataStream.flatMap(new IngestionMultiplierFlatMap(MULTI));

        DataStream<Estimation> estimationStream = DataStream2.keyBy((KeySelector<Datapoint, String>) Datapoint::getKey)
                .connect(SynopsisRequests.keyBy((KeySelector<Request, String>) Request::getKey))
                .flatMap(new SDEcoFlatMap()).name("SYNOPSES_MAINTENANCE").disableChaining();




        SplitStream<Estimation> split = estimationStream.split(new OutputSelector<Estimation>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<String> select(Estimation value) {
                // TODO Auto-generated method stub
                List<String> output = new ArrayList<>();
                if (value.getNoOfP() == 1) {
                    output.add("single");
                }
                else {
                    output.add("multy");
                }
                return output;
            }
        });

        DataStream<Estimation> single = split.select("single");
        DataStream<Estimation> multy = split.select("multy").keyBy((KeySelector<Estimation, String>) Estimation::getKey);
        //single.addSink(kp.getProducer());
        DataStream<Estimation> partialOutputStream = multy.flatMap(new ReduceFlatMap()).name("REDUCE").setParallelism(1);

        DataStream<Estimation> finalStream = partialOutputStream.flatMap(new GReduceFlatMap()).setParallelism(1);


        SplitStream<Estimation> split_2 = finalStream.split(new OutputSelector<Estimation>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<String> select(Estimation value) {
                // TODO Auto-generated method stub
                List<String> output = new ArrayList<>();
                if (value.getRequestID() == 7) {
                    output.add("UR");
                }
                else {
                    output.add("E");
                }
                return output;
            }
        });

        DataStream<Estimation> UR = split_2.select("UR");
        DataStream<Estimation> E = split_2.select("E");
        //E.addSink(kp.getProducer());
        //UR.addSink(pRequest.getProducer());

        finalStream.addSink(kp.getProducer()).setParallelism(1);
        env.execute("Streaming SDE"+ PARALLELISM +"_"+ MULTI +"_"+kafkaDataInputTopic);

    }

    private static void initializeParameters(String[] args) {

        if (args.length > 1) {

            System.out.println("[INFO] User Defined program arguments");
            //User defined program arguments
            kafkaDataInputTopic = args[0];
            kafkaRequestInputTopic = args[1];
            MULTI = Integer.parseInt(args[2]);
            PARALLELISM = Integer.parseInt(args[3]);
            SOURCE ="auto";
            kafkaBrokersList = "192.168.1.104:9093,192.168.1.104:9094";
            kafkaOutputTopic = "AIS_OUT";

            System.out.println("[INFO] SDE-E will execute with the following parameters at parallelism "+ PARALLELISM +":");
            System.out.println("\tKafka Brokers List: "+kafkaBrokersList);
            System.out.println("\tKafka Data Input Topic: "+kafkaDataInputTopic);
            System.out.println("\tKafka Request Input Topic: "+kafkaRequestInputTopic);
            System.out.println("\tKafka Output Topic: "+kafkaOutputTopic);

        }else{

            System.out.println("[INFO] Default values");
            //Default values
            kafkaDataInputTopic = "Dt_AIS";
            kafkaRequestInputTopic = "Rq_AIS";
            SOURCE ="auto";
            MULTI = 10;
            PARALLELISM = 1;
            kafkaBrokersList = "192.168.1.104:9093,192.168.1.104:9094";
            kafkaOutputTopic = "Estm_AIS";
            System.out.println("[INFO] SDE-E will execute with the following parameters at parallelism "+ PARALLELISM +":");
            System.out.println("\tKafka Brokers List: "+kafkaBrokersList);
            System.out.println("\tKafka Data Input Topic: "+kafkaDataInputTopic);
            System.out.println("\tKafka Request Input Topic: "+kafkaRequestInputTopic);
            System.out.println("\tKafka Output Topic: "+kafkaOutputTopic);

        }
    }
}
