package infore.SDE;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Message;
import infore.SDE.reduceFunctions.MessageReducer;
import infore.SDE.sources.KafkaProducerMessage;
import infore.SDE.sources.kafkaProducerEstimation;
import infore.SDE.sources.KafkaStringConsumer;

import infore.SDE.storage.StorageManager;
import infore.SDE.transformations.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.NoOpOperator;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import org.apache.flink.util.OutputTag;

@SuppressWarnings("deprecation")
public class Run {

	private static String kafkaDataInputTopic;
	private static String kafkaRequestInputTopic;
	private static String kafkaBrokersList;
	private static int parallelism;
	private static String kafkaOutputTopic;
	private static String kafkaLogTopic;

	/**
	 * OutputTag used to mark tuples headed to the logging side output stream. We need to create an {@link OutputTag}
	 * so we can reference it when catching emitted data from the side output at estimation stream.
	 */
	private static final OutputTag<Message> logOutputTag = new OutputTag<Message>("logging-tag") {};

	/**
	 * @param args Program arguments. You have to provide 6 arguments otherwise
	 *             DEFAULT values will be used.<br>
	 *             <ol>
	 *             <li>args[0]={@link #kafkaDataInputTopic} DEFAULT: "data")</li>
	 *             <li>args[1]={@link #kafkaRequestInputTopic} DEFAULT: "requests")</li>
	 *             <li>args[2]={@link #kafkaOutputTopic} DEFAULT: "outputs")</li>
	 *             <li>args[3]={@link #kafkaLogTopic} DEFAULT: "logging")</li>
	 *             <li>args[4]={@link #kafkaBrokersList} (DEFAULT: "localhost:9092")</li>
	 *             <li>args[5]={@link #parallelism} Job parallelism (DEFAULT: "4")</li>
	 *             </ol>
	 *
	 */
	public static void main(String[] args) throws Exception {
		// Initialize SDE Parameters
		initializeParameters(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaStringConsumer kc = new KafkaStringConsumer(kafkaBrokersList, kafkaDataInputTopic);
		KafkaStringConsumer requests = new KafkaStringConsumer(kafkaBrokersList, kafkaRequestInputTopic);
		kafkaProducerEstimation kp = new kafkaProducerEstimation(kafkaBrokersList, kafkaOutputTopic);
		KafkaProducerMessage kpmsg = new KafkaProducerMessage(kafkaBrokersList, kafkaLogTopic);

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
						return request;
					}
				}).name("REQUEST_SOURCE_STREAM").keyBy((KeySelector<Request, String>) Request::getKey);


		// Direct the request stream through the RqRouterFlatMap in order to generate SUB-Requests based on
		// number of parallelism 
		DataStream<Request> requestRouter = requestStream.flatMap(new RqRouterFlatMap()).name("REQUEST_ROUTER");

		// Connect the original request stream with the incoming data stream in order to allocate parallelism settings
		// for data tuples. The collector for this operation collects only data tuples with updated dataset keys.
		DataStream<Datapoint> dataRouter = dataStream.connect(requestStream)
				                                .flatMap(new DataRouterCoFlatMap()).name("DATA_ROUTER")
												.keyBy((KeySelector<Datapoint, String>) Datapoint::getKey);

		SingleOutputStreamOperator<Estimation> estimationStream = dataRouter.keyBy((KeySelector<Datapoint, String>) Datapoint::getKey)
				.connect(requestRouter.keyBy((KeySelector<Request, String>) Request::getKey))
				.process(new SDECoProcessFunction(true)).name("SYNOPSES_MAINTENANCE_CORE");

		// Get Messages emitted in the side output of the estimation stream by enforcing the tag.
		DataStream<Message> logs = estimationStream.getSideOutput(logOutputTag);

		// Apply no-op map to avoid deprecation errors in during split later.
		DataStream<Message> logsFiltered = logs
				.map(value -> value) // no-op map
				.name("LOG_FILTER");

		// Split the Messages to those having unitary parallelism and to those that were generated with parallelism > 1.
		SplitStream<Message> logsSplit = logsFiltered.split(new OutputSelector<Message>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> select(Message value) {
				List<String> output = new ArrayList<>();
				if (value.getParallelism() == 1) {
					output.add("single");
				} else {
					output.add("parallel");
				}
				return output;
			}
		});

		// Emit, directly to logging topic, messages with unitary parallelism without the need to reduce them.
		DataStream<Message> singleLogs = logsSplit.select("single");
		singleLogs.addSink(kpmsg.getProducer());

		// Reduce parallel messages into single message per external request identifier
		DataStream<Message> parallelMessages = logsSplit.select("parallel");

		DataStream<Message> aggregatedMessages = parallelMessages
				.keyBy(Message::getRelatedRequestIdentifier)
				.process(new MessageReducer()).setParallelism(1).name("MESSAGE_REDUCER");

		aggregatedMessages.addSink(kpmsg.getProducer());

		DataStream<Estimation> estimationFiltered = estimationStream
				.map(value -> value) // no-op mapm
				.name("ESTIMATION_FILTER");

		SplitStream<Estimation> split = estimationFiltered.split(new OutputSelector<Estimation>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> select(Estimation value) {
				List<String> output = new ArrayList<>();
				if (value.getNoOfP() == 1) {
					output.add("single");
				} else {
					output.add("parallel");
				}
				return output;
			}
		});

		DataStream<Estimation> single = split.select("single");
		single.addSink(kp.getProducer());

		DataStream<Estimation> parallel = split.select("parallel").keyBy((KeySelector<Estimation, String>) Estimation::getKey);
		DataStream<Estimation> partialOutputStream = parallel.flatMap(new ReduceFlatMap()).name("REDUCE");
		DataStream<Estimation> finalStream = partialOutputStream.flatMap(new GReduceFlatMap()).setParallelism(1);

		finalStream.addSink(kp.getProducer());

		env.execute("SynopsisDataEngine");
	}

	private static void initializeParameters(String[] args) {
		if (args.length > 5) {
			System.out.println("[INFO] Environment defined parameters");
			kafkaDataInputTopic = args[0];
			kafkaRequestInputTopic = args[1];
			kafkaOutputTopic = args[2];
			kafkaLogTopic = args[3];
			kafkaBrokersList = args[4];
			parallelism = Integer.parseInt(args[5]);
		} else{
			System.out.println("[INFO] Default parameters");
			kafkaDataInputTopic = "data";
			kafkaRequestInputTopic = "request";
			kafkaOutputTopic = "estimation";
			kafkaLogTopic = "logging";
			kafkaBrokersList = "sde.petrounetwork.gr:19092,sde.petrounetwork.gr:29092";
			parallelism = 2;
		}
	}
}
