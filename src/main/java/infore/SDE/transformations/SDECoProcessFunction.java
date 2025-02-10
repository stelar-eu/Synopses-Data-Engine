package infore.SDE.transformations;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Message;
import infore.SDE.messages.MessageType;
import infore.SDE.messages.Request;
import infore.SDE.storage.StorageManager;
import infore.SDE.synopses.*;
import lib.WDFT.controlBucket;
import lib.WLSH.Bucket;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hdfs.server.common.Storage;

/**
 * SDECoProcessFunction is a CoProcessFunction that consumes two keyed streams:
 * 1) A stream of {@link Datapoint} objects.
 * 2) A stream of {@link Request} objects.
 *
 * The class maintains a set of synopses keyed by the 'datasetKey' of each incoming Datapoint
 * or Request. When a Datapoint arrives, it is processed in {@code processElement1}, and when
 * a Request arrives, it is processed in {@code processElement2}. The two streams share state
 * via internal {@code HashMap} structures:
 *
 * - {@code M_Synopses} for non-continuous synopses
 * - {@code MC_Synopses} for continuous synopses
 *
 * This updated version also:
 * - Emits {@link Message} objects to a side output defined by {@code logOutputTag}.
 * - Optionally prints to stdout if {@code debugFlag} is set to {@code true}.
 */
public class SDECoProcessFunction extends CoProcessFunction<Datapoint, Request, Estimation> {

	private static final long serialVersionUID = 1L;

	/**
	 * HashMap for storing non-continuous synopses of specific DatasetKey
	 */
	private HashMap<String, ArrayList<Synopsis>> M_Synopses = new HashMap<>();

	/**
	 * HashMap for storing continuous synopses of specific DatasetKey
	 */
	private HashMap<String, ArrayList<ContinuousSynopsis>> MC_Synopses = new HashMap<>();

	/**
	 * OutputTag used to mark tuples headed to the logging side output stream.
	 */
	private static final OutputTag<Message> logOutputTag = new OutputTag<Message>("logging-tag") {};

	/**
	 * A flag to enable or disable printing to stdout.
	 */
	private final boolean debugFlag;



	/**
	 * Optional processor ID (just a demonstration of how you might handle multi-parallel operators).
	 */
	private int pId;

	/**
	 * Constructor.
	 *
	 * @param debugFlag if true, the operator will print messages to stdout in addition to emitting them to the side output.
	 */
	public SDECoProcessFunction(boolean debugFlag) {
		this.debugFlag = debugFlag;
	}

	/**
	 * Helper method to emit a {@link Message} to the side output and optionally print to stdout.
	 *
	 * @param ctx     The context to emit the message to side output.
	 * @param level   A short string or type describing the message level, e.g. "INFO", "ERROR", etc.
	 * @param info    The string to either log or print to stdout.
	 * @param request (Optional) if we want to attach the request ID/key to the message. If null, we skip request-based fields.
	 */
	private void logOrOutput(Context ctx, String level, Object info, Request request) {

		// Build content for side output
		String uidKey = "";
		int reqId = -1;
		if (request != null) {
			uidKey = request.getUID() + "_" + request.getKey();
			reqId = request.getRequestID();
		} else {
			// Just a fallback
			uidKey = "NO_REQUEST";
		}

		MessageType type = MessageType.RESPONSE;

		if (level.equals("INFO")) {
			type = MessageType.RESPONSE;
		} else if (level.equals("ERROR")){
			type = MessageType.ERROR;
		}
		// Create the Message object
		Message msg = new Message(type, info, request.getExternalUID(), reqId, request.getDataSetkey(), request.getNoOfP());

		// Emit to side output
		ctx.output(logOutputTag, msg);

		// Optionally print to stdout
		if (debugFlag) {
			System.out.println("[" + level + "] " + info);
		}
	}

	private String maskKey(String input){
		//  _<digits>_KEYED_<digits>  OR  _<digits>_RANDOM_<digits>
		String regex = "_\\d+_KEYED_\\d+|_\\d+_RANDOM_\\d+";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(input);

		if (matcher.find()) {
			// Keep only the part of the string before the match
			return input.substring(0, matcher.start());
		}
		return input; // If no match, return the original string
	}

	/**
	 * processElement1 handles incoming Datapoints.
	 *
	 * @param node The Datapoint.
	 * @param ctx The CoProcessFunction context.
	 * @param collector The collector for Estimation outputs.
	 * @throws JsonProcessingException
	 */
	@Override
	public void processElement1(Datapoint node, Context ctx, Collector<Estimation> collector) throws JsonProcessingException {
		//Get the possible synopses for this dataset key
		ArrayList<Synopsis> Synopses = M_Synopses.get(node.getKey());

		// If we have non-continuous synopses for this key, add the new datapoint
		if (Synopses != null) {
			for (Synopsis ski : Synopses) {
				try {
					ski.add(node.getValues());
				} catch (Exception e) {
					e.printStackTrace();
					logOrOutput(ctx, "ERROR", "Exception in adding Datapoint: " + e.getMessage(), null);
				}
			}
			M_Synopses.put(node.getKey(), Synopses);
		}

		ArrayList<ContinuousSynopsis> C_Synopses = MC_Synopses.get(node.getKey());
		if (C_Synopses != null) {
			for (ContinuousSynopsis c_ski : C_Synopses) {
				// addEstimate for continuous synopses
				Estimation e = c_ski.addEstimate(node.getValues());
				if (e != null && e.getEstimation() != null) {
					collector.collect(e);
				}
				//Get the CONTINUOUS synopses for this key
			}
			MC_Synopses.put(node.getKey(), C_Synopses);
		}
	}

	/**
	 * processElement2 handles incoming Requests.
	 *
	 * @param rq The Request.
	 * @param ctx The context object.
	 * @param collector The output collector for Estimation results.
	 * @throws Exception
	 */
	@Override
	public void processElement2(Request rq, Context ctx, Collector<Estimation> collector) throws Exception {

		// Provide some logging/side-output
		// logOrOutput(ctx, "INFO", "Will handle request: " + rq.toString(), rq);

		//Get synopses for the key
		ArrayList<Synopsis> Synopses = M_Synopses.get(rq.getKey());
		ArrayList<ContinuousSynopsis> C_Synopses = MC_Synopses.get(rq.getKey());

		// Handle special request IDs
		if (rq.getRequestID() == 100) {
			//Snapshot a specific synopsis instance
			boolean snapshotFlag = false;
			if (Synopses != null) {
				for (Synopsis s : Synopses) {
					if (s.getSynopsisID() == rq.getUID()) {
						if(!StorageManager.isInitialized()){
							logOrOutput(ctx, "ERROR", "StorageManager is not initialized. Aborting snapshot capture!", rq);
							snapshotFlag = true;
						}else {
							if (StorageManager.snapshotSynopsis(s, rq.getKey())) {
								logOrOutput(ctx, "INFO", "Snapshot Synopsis Success, UID: " + s.getSynopsisID()
										+ " | Snapshot at: " + LocalDateTime.now().toString(), rq);
								snapshotFlag = true;
							} else {
								logOrOutput(ctx, "ERROR", "Snapshot Synopsis Failed: [ Internal Storage Error ]", rq);
							}
						}
					}
				}
			}
			if(!snapshotFlag){
				logOrOutput(ctx, "ERROR", "Snapshot Synopsis Failed: Could not find synopsis with UID: " + rq.getUID(), rq);
			}
		}
		else if (rq.getRequestID() == 101) {
			// Change the Object Store on the fly (not implemented in snippet)

			try {
				String type = rq.getParam(0).toString();
				if(type.equals("sts")){
					String accessKey = rq.getParam(1).toString();
					String secretKey = rq.getParam(2).toString();
					String sessionToken = rq.getParam(3).toString();
					String endpoint = rq.getParam(4).toString();
					StorageManager.initialize(endpoint, accessKey, secretKey, sessionToken);
					logOrOutput(ctx, "INFO", "Updated StorageManager Credentials", rq);
				}
			} catch (Exception e){
				logOrOutput(ctx, "ERROR", "Exception occurred while configuring StorageManager : "+e, rq);
			}
		}
		else if (rq.getRequestID() == 301) {
			// Return the snapshots of a given synopsis
			try {
				List<String> snapshots = StorageManager.getSynopsisVersions(rq.getUID(), rq.getKey(), true);
				logOrOutput(ctx, "INFO", snapshots.toString(), rq);
			} catch (Exception e){
				logOrOutput(ctx, "ERROR", "Exception occurred while configuring StorageManager : "+e, rq);
			}
		}
		else if (rq.getRequestID() == 201 || rq.getRequestID() == 200) {
			// Load snapshot into currently running instance of synopsis
			// Then replace the existing instance in M_Synopses
			if (Synopses == null) {
				logOrOutput(ctx, "WARN", "No synopses exist for this key; cannot load snapshot.", rq);
				return;
			}

			// Handle CountMin
			if (rq.getSynopsisID() == 1) {
				try {
					CountMin loadedObj = null;
					if(rq.getRequestID() == 201) {
						// load specific version
						loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), CountMin.class, Integer.valueOf(rq.getParam()[0]));
						loadedObj.setParallelism(rq.getNoOfP());
						loadedObj.setKey(rq.getDataSetkey());
					} else {
						// load latest version
						loadedObj = StorageManager.loadSynopsisLatestSnapshot(rq.getKey(), rq.getUID(), CountMin.class);
						loadedObj.setParallelism(rq.getNoOfP());
						loadedObj.setKey(rq.getDataSetkey());
					}

					for (int i = 0; i < Synopses.size(); i++) {
						Synopsis s = Synopses.get(i);
						if (s.getSynopsisID() == rq.getUID()) {
							Synopses.remove(s);
							Synopses.add(loadedObj);

							String vLoaded = (rq.getRequestID() == 200)
									? String.valueOf(StorageManager.getSynopsisLatestVersionNumber(rq.getUID(), rq.getKey()))
									: rq.getParam()[0];

							logOrOutput(ctx, "INFO", "Loaded snapshot of synopsis: Version : v" + vLoaded + " UID: " + rq.getUID() + " Type: CountMin ", rq);
							break;
						}
					}
					M_Synopses.put(rq.getKey(), Synopses);

				} catch(Exception e){
					e.printStackTrace();
					logOrOutput(ctx, "ERROR", "Failed to load CountMin snapshot: " + e.getMessage(), rq);
				}
			}
			// BloomFilter
			else if (rq.getSynopsisID()==2){
				try {
					Bloomfilter loadedObj = null;
					if(rq.getRequestID() == 201) {
						loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), Bloomfilter.class, Integer.valueOf(rq.getParam()[0]));
						loadedObj.setParallelism(rq.getNoOfP());
						loadedObj.setKey(rq.getDataSetkey());
					} else {
						loadedObj = StorageManager.loadSynopsisLatestSnapshot(rq.getKey(), rq.getUID(), Bloomfilter.class);
						loadedObj.setParallelism(rq.getNoOfP());
						loadedObj.setKey(rq.getDataSetkey());
					}

					for (int i = 0; i < Synopses.size(); i++) {
						Synopsis s = Synopses.get(i);
						if (s.getSynopsisID() == rq.getUID()) {
							Synopses.remove(s);
							Synopses.add(loadedObj);

							String vLoaded = (rq.getRequestID() == 200)
									? String.valueOf(StorageManager.getSynopsisLatestVersionNumber(rq.getUID(), rq.getKey()))
									: rq.getParam()[0];

							logOrOutput(ctx, "INFO", "Loaded snapshot of synopsis: Version : v" + vLoaded + " UID: " + rq.getUID() + " Type: BloomFiter ", rq);
							break;
						}
					}
					M_Synopses.put(rq.getKey(), Synopses);

				} catch(Exception e){
					e.printStackTrace();
					logOrOutput(ctx, "ERROR", "Failed to load BloomFilter snapshot: " + e.getMessage(), rq);
				}
			}
			// AMSsynopsis
			else if (rq.getSynopsisID()==3){
				try {
					AMSsynopsis loadedObj = null;
					if(rq.getRequestID() == 201) {
						loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), AMSsynopsis.class, Integer.valueOf(rq.getParam()[0]));
						loadedObj.setParallelism(rq.getNoOfP());
						loadedObj.setKey(rq.getDataSetkey());
					} else {
						loadedObj = StorageManager.loadSynopsisLatestSnapshot(rq.getKey(), rq.getUID(), AMSsynopsis.class);
						loadedObj.setParallelism(rq.getNoOfP());
						loadedObj.setKey(rq.getDataSetkey());
					}

					for (int i=0; i<Synopses.size(); i++) {
						Synopsis s = Synopses.get(i);
						if (s.getSynopsisID() == rq.getUID()) {
							Synopses.remove(s);
							Synopses.add(loadedObj);

							String vLoaded = (rq.getRequestID() == 200)
									? String.valueOf(StorageManager.getSynopsisLatestVersionNumber(rq.getUID(), rq.getKey()))
									: rq.getParam()[0];

							logOrOutput(ctx, "INFO", "Loaded snapshot of synopsis: Version : v" + vLoaded + " UID: " + rq.getUID() + " Type: AMSSynopsis ", rq);

							break;
						}
					}
					M_Synopses.put(rq.getKey(), Synopses);

				} catch(Exception e){
					e.printStackTrace();
					logOrOutput(ctx, "ERROR", "Failed to load AMSSynopsis snapshot: " + e.getMessage(), rq);
				}
			}
		}
		else if (rq.getRequestID() == 202) {
			// Load snapshot into a newly instantiated synopsis
			if (Synopses == null) {
				Synopses = new ArrayList<>();
			}

			if(rq.getSynopsisID()==1){
				try {
					CountMin loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), CountMin.class, Integer.valueOf(rq.getParam()[0]));
					loadedObj.setSynopsisID(Integer.valueOf(rq.getParam()[1]));
					loadedObj.setParallelism(rq.getNoOfP());
					loadedObj.setKey(rq.getDataSetkey());
					Synopses.add(loadedObj);
					logOrOutput(ctx, "INFO",
							"Loaded snapshot of synopsis: UID: "+rq.getUID()+" into new instance:  Version : v" + rq.getParam()[0] +" to Synopsis New UID: " + rq.getParam()[1] +" of type: CountMin", rq);
					M_Synopses.put(rq.getKey(), Synopses);
				}catch(Exception e){
					e.printStackTrace();
					logOrOutput(ctx, "ERROR", "Failed to load new CountMin instance: " + e.getMessage(), rq);
				}
			}
			else if(rq.getSynopsisID()==2){
				try {
					Bloomfilter loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), Bloomfilter.class, Integer.valueOf(rq.getParam()[0]));
					loadedObj.setSynopsisID(Integer.valueOf(rq.getParam()[1]));
					loadedObj.setParallelism(rq.getNoOfP());
					loadedObj.setKey(rq.getDataSetkey());
					Synopses.add(loadedObj);
					logOrOutput(ctx, "INFO",
							"Loaded snapshot of synopsis: UID: "+rq.getUID()+" into new instance:  Version : v" + rq.getParam()[0] +" to Synopsis New UID: " + rq.getParam()[1] +" of type: BloomFilter", rq);
					M_Synopses.put(rq.getKey(), Synopses);
				}catch(Exception e){
					e.printStackTrace();
					logOrOutput(ctx, "ERROR", "Failed to load new BloomFilter instance: " + e.getMessage(), rq);
				}
			}
			else if(rq.getSynopsisID()==3){
				try {
					AMSsynopsis loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), AMSsynopsis.class, Integer.valueOf(rq.getParam()[0]));
					loadedObj.setSynopsisID(Integer.valueOf(rq.getParam()[1]));
					loadedObj.setParallelism(rq.getNoOfP());
					loadedObj.setKey(rq.getDataSetkey());
					Synopses.add(loadedObj);
					logOrOutput(ctx, "INFO",
							"Loaded snapshot of synopsis: UID: "+rq.getUID()+" into new instance:  Version : v" + rq.getParam()[0] +" to Synopsis New UID: " + rq.getParam()[1] +" of type: AMSSynopsis", rq);
					M_Synopses.put(rq.getKey(), Synopses);
				}catch(Exception e){
					e.printStackTrace();
					logOrOutput(ctx, "ERROR", "Failed to load new AMSSynopsis instance: " + e.getMessage(), rq);
				}
			}
		}
		else if (rq.getRequestID() == 1000) {
			// Show metadata for all maintained synopses
			try {

				Map<String, List<Map<String, Object>>> groupedSynopses = new HashMap<>();


				for (String datasetKey : M_Synopses.keySet()) {
					List<Map<String, Object>> synopsisList = new ArrayList<>();

					for (Synopsis s : M_Synopses.get(datasetKey)) {
						Map<String, Object> synopsisMap = new HashMap<>();
						synopsisMap.put("id", s.getSynopsisID());
						synopsisMap.put("type", s.getClass().getSimpleName());
						synopsisMap.put("parallelism", s.getParallelism());
						synopsisMap.put("key", maskKey(s.getKey()));
						synopsisList.add(synopsisMap);
					}
					groupedSynopses.merge(maskKey(datasetKey), synopsisList, (oldList, newList) -> {
						oldList.addAll(newList); // Merge new list into existing one
						return oldList; // Return the updated list
					});
				}


				logOrOutput(ctx, "INFO", groupedSynopses, rq);

			} catch (Exception e) {
				e.printStackTrace();
				logOrOutput(ctx, "ERROR", "Metadata request failed: " + e.getMessage(), rq);
			}
		}

		/**
		 * Request ID:1 -> Add synopsis with keyed partitioning (non continuous)
		 * Request ID:4 -> Add synopsis with random partitioning (non continuous)
		 */
		else if (rq.getRequestID() == 1 || rq.getRequestID() == 4 ) {
			if(Synopses == null){
				Synopses = new ArrayList<>();
			}

			Synopsis newSketch = null;
			switch (rq.getSynopsisID()) {
				case 1:
					// CountMin
					if (rq.getParam().length > 4) {
						newSketch = new CountMin(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new CountMin synopsis with UID: "+rq.getUID() + " and params: "+Arrays.toString(rq.getParam()), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for CountMin. Will not add new Synopsis.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 2:
					// BloomFilter
					if (rq.getParam().length > 3) {
						newSketch = new Bloomfilter(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new BloomFilter synopsis with UID: "+rq.getUID() + " and params: "+Arrays.toString(rq.getParam()), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for BloomFilter. Will not add new Synopsis.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 3:
					// AMSSynopsis
					if (rq.getParam().length > 3){
						newSketch = new AMSsynopsis(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new AMSSketch synopsis with UID: "+rq.getUID() + " and params: "+Arrays.toString(rq.getParam()), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for AMSketch. Not adding new instance.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 4:
					// DFT
					if (rq.getParam().length > 3){
						newSketch = new MultySynopsisDFT(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new DFT synopsis [Type ID: "
								+ rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()
								+"] upon request: " + rq.getUID(), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for DFT. Not adding new instance.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 5:
					//lsh
					//notimplemented
					break;
				case 6:
					// lib.Coresets
					if (rq.getParam().length > 10 ){
						newSketch = new FinJoinCoresets(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new Coresets synopsis [Type ID: "
								+ rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()
								+"] upon request: " + rq.getUID(), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for Coresets. Not adding new instance.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 7:
					// HyperLogLog
					if (rq.getParam().length > 2){
						newSketch = new HyperLogLogSynopsis(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new HyperLogLog synopsis [Type ID: "
								+ rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()
								+"] upon request: " + rq.getUID(), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for HyperLogLog. Not adding new instance.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 8:
					// StickySampling
					if (rq.getParam().length > 4) {
						newSketch = new StickySamplingSynopsis(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new StickySampling synopsis [Type ID: "
								+ rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()
								+"] upon request: " + rq.getUID(), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for StickySampling. Not adding new instance.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 9:
					// LossyCounting
					if (rq.getParam().length > 2){
						newSketch = new LossyCountingSynopsis(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new LossyCounting synopsis [Type ID: "
								+ rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()
								+"] upon request: " + rq.getUID(), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for LossyCounting. Not adding new instance.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 10:
					// ChainSampler
					if (rq.getParam().length > 3){
						newSketch = new ChainSamplerSynopsis(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new ChainSampler synopsis [Type ID: "
								+ rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()
								+"] upon request: " + rq.getUID(), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for ChainSampler. Not adding new instance.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 11:
					// GKsynopsis
					if (rq.getParam().length > 3){
						newSketch = new GKsynopsis(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new GK synopsis [Type ID: "
								+ rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()
								+"] upon request: " + rq.getUID(), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for GK. Not adding new instance.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 13:
					// Top-K
					if (rq.getParam().length > 3){
						newSketch = new SynopsisTopK(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new Top-K synopsis [Type ID: "
								+ rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()
								+"] upon request: " + rq.getUID(), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for Top-K. Not adding new instance.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 16:
					// windowQuantiles
					if (rq.getParam().length > 3){
						newSketch = new windowQuantiles(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
						logOrOutput(ctx, "INFO", "Maintaining new WindowQuantiles synopsis [Type ID: "
								+ rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()
								+"] upon request: " + rq.getUID(), rq);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for WindowQuantiles. Not adding new instance.", rq);
					}
					Synopses.add(newSketch);
					break;
				case 25:
					// dynamic load from jar
					Object instance;
					if (rq.getParam().length == 4) {
						File myJar = new File(rq.getParam()[2]);
						URLClassLoader child = new URLClassLoader(new URL[]{myJar.toURI().toURL()},
								this.getClass().getClassLoader());
						Class<?> classToLoad = Class.forName(rq.getParam()[3], true, child);
						instance = classToLoad.getConstructor().newInstance();
						Synopses.add((Synopsis) instance);
					} else {
						File myJar = new File("C:\\Users\\flinkSketches.jar");
						URLClassLoader child = new URLClassLoader(new URL[]{myJar.toURI().toURL()},
								this.getClass().getClassLoader());
						Class<?> classToLoad = Class.forName("com.yahoo.sketches.sampling.NewSketch", true, child);
						instance = classToLoad.getConstructor().newInstance();
						Synopses.add((Synopsis) instance);
					}
					logOrOutput(ctx, "INFO", "Dynamically loaded new synopsis from jar with ID 25", rq);
					break;
				case 26:
					// FINJOIN
					if (rq.getParam().length > 3) {
						newSketch = new FinJoinSynopsis(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
					}
					Synopses.add(newSketch);
					logOrOutput(ctx, "INFO", "Maintaining new FinJoinSynopsis with ID 26", rq);
					break;
				case 27:
					// COUNT
					if (rq.getParam().length > 3) {
						newSketch = new Counters(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
					} else {
						String[] _tmp = {"0", "0", "10", "100", "8", "3"};
						newSketch = new Counters(rq.getUID(), _tmp);
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
					}
					Synopses.add(newSketch);
					logOrOutput(ctx, "INFO", "Maintaining new Counters with ID 27", rq);
					break;
				case 28:
					// Window LSH
					if (rq.getParam().length > 3) {
						newSketch = new WLSHSynopses(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
					}
					Synopses.add(newSketch);
					logOrOutput(ctx, "INFO", "Maintaining new WLSHSynopses with ID 28", rq);
					break;
				case 29:
					// PastDFT
					if (rq.getParam().length > 3) {
						newSketch = new PastDFTSynopsis(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
					}
					Synopses.add(newSketch);
					logOrOutput(ctx, "INFO", "Maintaining new PastDFTSynopsis with ID 29", rq);
					break;
				case 30:
					// SpatialSketch
					if (rq.getParam().length > 4) {
						newSketch = new SpatialSketch(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
					}
					Synopses.add(newSketch);
					logOrOutput(ctx, "INFO", "Maintaining new SpatialSketch with ID 30", rq);
					break;
				case 31:
					// OmniSketch
					if (rq.getParam().length > 6) {
						newSketch = new OmniSketch(rq.getUID(), rq.getParam());
						newSketch.setParallelism(rq.getNoOfP());
						newSketch.setKey(rq.getDataSetkey());
					}
					Synopses.add(newSketch);
					logOrOutput(ctx, "INFO", "Maintaining new OmniSketch with ID 31", rq);
					break;
				default:
					logOrOutput(ctx, "WARN", "SynopsisID not recognized: " + rq.getSynopsisID(), rq);
					break;
			}

			M_Synopses.put(rq.getKey(),Synopses);
		}

		/**
		 * Request ID:5 -> Add continuous synopsis
		 */
		else if(rq.getRequestID() == 5) {
			if (C_Synopses == null){
				C_Synopses = new ArrayList<>();
			}
			ContinuousSynopsis sketch = null;

			switch (rq.getSynopsisID()) {
				case 1:
					// ContinuousCM
					if (rq.getParam().length > 4) {
						sketch = new ContinuousCM(rq.getUID(), rq, rq.getParam());
						logOrOutput(ctx, "INFO", "Maintaining new ContinuousCM with ID 1", rq);
						C_Synopses.add(sketch);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for ContinuousCM. Not adding new instance.", rq);
					}
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;
				case 100:
					// Radius_Grid
					if (rq.getParam().length > 4) {
						sketch = new Radius_Grid(rq);
						logOrOutput(ctx, "INFO", "Maintaining new Radius_Grid with ID 100", rq);
						C_Synopses.add(sketch);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient parameters for Radius_Grid. Not adding new instance.", rq);
					}
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;
				case 12:
					// ContinuousMaritimeSketch
					if (rq.getParam().length > 5) {
						rq.setNoOfP(1);
						sketch = new ContinuousMaritimeSketches(rq.getUID(), rq, rq.getParam());
						logOrOutput(ctx, "INFO", "Maintaining new ContinuousMaritimeSketch with ID 12", rq);
						C_Synopses.add(sketch);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient params for ContinuousMaritimeSketch. Not adding new instance.", rq);
					}
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;
				case 15:
					// ISWoR
					if (rq.getParam().length > 5) {
						sketch = new ISWoR(rq.getUID(), rq, rq.getParam());
						logOrOutput(ctx, "INFO", "Maintaining new ISWoR with ID 15", rq);
						C_Synopses.add(sketch);
					} else {
						logOrOutput(ctx, "ERROR", "Insufficient params for ISWoR. Not adding new instance.", rq);
					}
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;
				default:
					logOrOutput(ctx, "WARN", "Unsupported continuous synopsis ID: " + rq.getSynopsisID(), rq);
					break;
			}
		}
		else {
			// Possibly: requestID=2 -> Delete a synopsis
			// Possibly: requestID=3 or 6 -> Query a synopsis
			// Possibly: requestID=7 -> Update a synopsis
			if(Synopses==null){
				logOrOutput(ctx, "WARN", "No synopsis found for dataset key: " + rq.getKey(), rq);
			} else {
				for (int i = 0; i < Synopses.size(); i++) {
					Synopsis syn = Synopses.get(i);
					if (rq.getUID() == syn.getSynopsisID()) {
						// remove (2)
						if (rq.getRequestID() == 2) {
							Synopses.remove(syn);
							M_Synopses.put(rq.getKey(), Synopses);
							logOrOutput(ctx, "INFO", "Deleted synopsis of Type with UID: " +rq.getUID() + " and Type: " +rq.getSynopsisID(), rq);
							break;
						}
						// estimate (3 or 6)
						else if ((rq.getRequestID() % 10 == 3) || (rq.getRequestID() % 10 == 6)) {
							Estimation e = syn.estimate(rq);
							if (e.getEstimation() != null) {
								if (rq.getSynopsisID() == 28) {
									// special handling for WLSHSynopses
									HashMap<Integer, Bucket> buckets = (HashMap<Integer, Bucket>) e.getEstimation();
									for (Map.Entry<Integer, Bucket> entry : buckets.entrySet()) {
										Integer key = entry.getKey();
										Bucket value = entry.getValue();
										String info = "Bucket No. -> " + key + " PId:" + pId + "\n INFO -> " + value.toString();
										logOrOutput(ctx, "INFO", info, rq);

										e.setKey(e.getUID() + "_" + key);
										e.setEstimationkey(e.getUID() + "_" + key + "_" + pId);
										e.setEstimation(value);
										collector.collect(e);
									}
								} else if (rq.getSynopsisID() == 29) {
									// special handling for PastDFTSynopsis
									HashMap<String, controlBucket> buckets = (HashMap<String, controlBucket>) e.getEstimation();
									for (Map.Entry<String, controlBucket> entry : buckets.entrySet()) {
										String key = entry.getKey();
										controlBucket value = entry.getValue();
										if (value != null) {
											String info = "Bucket BEFORE with KEY ->" + key + " INFO -> " + value.toString();
											logOrOutput(ctx, "INFO", info, rq);
										}

										e.setKey(key);
										e.setEstimationkey(e.getUID() + "_" + key + "_" + pId);
										e.setEstimation(value);
										collector.collect(e);
									}
								} else {
									if(rq.getExternalUID()!=null){
										e.setRelatedRequestIdentifier(rq.getExternalUID());
									}
									collector.collect(e);
								}
							}
						}
						// requestID=7 => possible update logic, but not fully used in snippet
					}
				}
			}
		}
	}
}