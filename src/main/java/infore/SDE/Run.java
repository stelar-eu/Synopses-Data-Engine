package infore.SDE;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Datapoint;
import infore.SDE.sources.kafkaProducerEstimation;
import infore.SDE.sources.KafkaStringConsumer;

import infore.SDE.transformations.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;


/**
 * <br>
 * Implementation code for SDE for INFORE-PROJECT" <br> *
 * ATHENA Research and Innovation Center <br> *
 * Author: Antonis_Kontaxakis <br> *
 * email: adokontax15@gmail.com *
 */

@SuppressWarnings("deprecation")
public class Run {

	private static String kafkaDataInputTopic;
	private static String kafkaRequestInputTopic;
	private static String kafkaBrokersList;
	private static int parallelism;
	private static String kafkaOutputTopic;

	/**
	 * @param args Program arguments. You have to provide 4 arguments otherwise
	 *             DEFAULT values will be used.<br>
	 *             <ol>
	 *             <li>args[0]={@link #kafkaDataInputTopic} DEFAULT: "Forex")
	 *             <li>args[1]={@link #kafkaRequestInputTopic} DEFAULT: "Requests")
	 *             <li>args[2]={@link #kafkaBrokersList} (DEFAULT: "localhost:9092")
	 *             <li>args[3]={@link #parallelism} Job parallelism (DEFAULT: "4")
	 *             <li>args[4]={@link #kafkaOutputTopic} DEFAULT: "OUT")
	 *             "O10")
	 *             </ol>
	 *
	 */

	public static void main(String[] args) throws Exception {
		// Initialize Input Parameters
		initializeParameters(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		KafkaStringConsumer kc = new KafkaStringConsumer(kafkaBrokersList, kafkaDataInputTopic);
		KafkaStringConsumer requests = new KafkaStringConsumer(kafkaBrokersList, kafkaRequestInputTopic);
		kafkaProducerEstimation kp = new kafkaProducerEstimation(kafkaBrokersList, kafkaOutputTopic);

		DataStream<String> stringDataStream = env.addSource(kc.getFc());
		DataStream<String> stringRequestStream = env.addSource(requests.getFc());

		// Map and transform Kafka input for request topic to a stream of Datapoint objects
		DataStream<Datapoint> dataStream = stringDataStream
				.map(new MapFunction<String, Datapoint>() {
					@Override
					public Datapoint map(String node) throws IOException {
						//Use object mapper from jackson to map the string to a Datapoint object
						ObjectMapper objectMapper = new ObjectMapper();

						Datapoint dp = objectMapper.readValue(node, Datapoint.class);
						return dp;
					}
				}).name("DATA_SOURCE_STREAM").keyBy((KeySelector<Datapoint, String>)Datapoint::getKey);

		// Map and transform Kafka input for data topic to a stream of Request objects
		DataStream<Request> requestStream = stringRequestStream
				.map(new MapFunction<String, Request>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Request map(String node) throws IOException {
						//Use object mapper from jackson to map the string to a Request object
						ObjectMapper objectMapper = new ObjectMapper();

						Request request = objectMapper.readValue(node, Request.class);
						return  request;
					}
				}).name("REQUEST_SOURCE_STREAM").keyBy((KeySelector<Request, String>) Request::getKey);


		// Direct the request stream through a router resulting
		DataStream<Request> requestRouter = requestStream.flatMap(new RqRouterFlatMap()).name("REQUEST_ROUTER");

		DataStream<Datapoint> dataRouter = dataStream.connect(requestStream)
				                                .flatMap(new DataRouterCoFlatMap()).name("DATA_ROUTER")
												.keyBy((KeySelector<Datapoint, String>) Datapoint::getKey);

		DataStream<Estimation> estimationStream = dataRouter.keyBy((KeySelector<Datapoint, String>) Datapoint::getKey)
				.connect(requestRouter.keyBy((KeySelector<Request, String>) Request::getKey))
				.flatMap(new SDEcoFlatMap()).name("SYNOPSES_MAINTENANCE_CORE");


		SplitStream<Estimation> split = estimationStream.split(new OutputSelector<Estimation>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> select(Estimation value) {
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
		single.addSink(kp.getProducer());

		DataStream<Estimation> partialOutputStream = multy.flatMap(new ReduceFlatMap()).name("REDUCE");

		DataStream<Estimation> finalStream = partialOutputStream.flatMap(new GReduceFlatMap()).setParallelism(1);
		finalStream.print();

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

		finalStream.addSink(kp.getProducer());
		env.execute("Streaming SDE");

	}

	private static void initializeParameters(String[] args) {

		if (args.length > 4) {
			System.out.println("[INFO] User defined parameters");
			//User defined program arguments
			kafkaDataInputTopic = args[0];
			kafkaRequestInputTopic = args[1];
			kafkaOutputTopic = args[2];

			kafkaBrokersList = args[3];
			parallelism = Integer.parseInt(args[4]);
		}else{
			System.out.println("[INFO] Default parameters");
			//Default values
			kafkaDataInputTopic = "data_topic";
			kafkaRequestInputTopic = "request_topic";
			kafkaOutputTopic = "estimation_topic";

			kafkaBrokersList = "192.168.1.104:9093,192.168.1.104:9094";
			parallelism = 1;
		}
	}
}
