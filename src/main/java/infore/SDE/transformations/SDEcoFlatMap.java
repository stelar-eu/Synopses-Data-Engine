package infore.SDE.transformations;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.LocalDateTime;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import infore.SDE.storage.StorageManager;
import infore.SDE.storage.StorageManagerMinIO;
import lib.WDFT.controlBucket;
import lib.WLSH.Bucket;
import infore.SDE.synopses.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.messages.Datapoint;
import org.apache.hadoop.fs.shell.Count;

/**
 * The SDEcoFlatMap is used after the connection of the dataStream with the requestStream and after
 * both of them have been keyed by DataSetKey attribute. It handles arriving DataPoints and Requests.
 * We use a CoFlatMap type of operator because we need to have a shared state between the 2 streams.
 * The shared state allows both streams to have access to synopses maintenance ArrayLists and perform
 * add or put operations on them.
 */
public class SDEcoFlatMap extends RichCoFlatMapFunction<Datapoint, Request, Estimation> {

	private static final long serialVersionUID = 1L;
	/**
	 * HashMap for storing non-continuous synopses of specific DatasetKey
	 */
	private HashMap<String,ArrayList<Synopsis>> M_Synopses = new HashMap<>();
	/**
	 * HashMap for storing continuous synopses of specific DatasetKey
	 */
	private HashMap<String,ArrayList<ContinuousSynopsis>> MC_Synopses = new HashMap<>();


	private int pId;

	/**
	 * This flatMap1 method is called in case a Datapoint arrives from the stream
	 *
	 * @param node The stream element in the case of flatMap1 is a Datapoint
	 * @param collector The collector to emit resulting elements
	 * @throws JsonProcessingException
	 */
	@Override
	public void flatMap1(Datapoint node, Collector<Estimation> collector) throws JsonProcessingException {
		//Get the possible synopses (based on the node key, dataSetKey) in which
		//the new Datapoint should be included
		ArrayList<Synopsis>  Synopses =  M_Synopses.get(node.getKey());

		//In case there are Synopses that can accept the node (based on dataSetKey)
		//add the node to each of them

		// TODO: Here upon a specific criterion, a snapshot of the updated synopsis must be saved into the storage service

		if (Synopses != null) {
			for (Synopsis ski : Synopses) {
				try {
					ski.add(node.getValues());
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			M_Synopses.put(node.getKey(),Synopses);
		}


		//Get the CONTINUOUS possible synopses (based on the node key, dataSetKey) in which
		//the new Datapoint should be included
		ArrayList<ContinuousSynopsis>  C_Synopses =  MC_Synopses.get(node.getKey());
		//In case there are CONTINUOUS Synopses that can accept the node (based on dataSetKey)
		//add the node to each of them
		if (C_Synopses != null) {
			for (ContinuousSynopsis c_ski : C_Synopses) {

				//We use the addEstimate because continuous synopses are updated in real time,
				//we expect from them an estimation right after the coming of new data. Simultaneously
				//the new datapoint is added and an estimation is being produced. (collected by the collector)
				Estimation e =c_ski.addEstimate(node.getValues());
				if(e!=null){
					if(e.getEstimation()!=null)
						collector.collect(e);
				}
			}
			//Update the arrayList of continuous synopses with the updated ones
			//(replaces the value if key is already present in the HashMap)
			MC_Synopses.put(node.getKey(),C_Synopses);
		}
	}

	/**
	 * This flatMap2 method is called in case a Request arrives from the stream
	 * @param rq The stream element in the case of flatMap2 is a Request
	 * @param collector The collector to emit resulting elements to
	 * @throws Exception
	 */
	@Override
	public void flatMap2(Request rq, Collector<Estimation> collector) throws Exception {

		System.out.println("[INFO] Will handle request: "+rq.toString());

		//Get the already maintained synopses (regular and continuous) for the given DataSetKey
		ArrayList<Synopsis>  Synopses =  M_Synopses.get(rq.getKey());
		ArrayList<ContinuousSynopsis>  C_Synopses =  MC_Synopses.get(rq.getKey());

		/**
		 * Request ID:100 --> Snapshot synopsis with specific UID and DatasetKey
		 */
		if (rq.getRequestID() == 100) {
			boolean snapshotFlag = false;
			// Snapshot possible existent non-continuous synopsis
			for(Synopsis s: M_Synopses.get(rq.getKey())){
				if(s.getSynopsisID() == rq.getUID()){
					if(StorageManager.snapshotSynopsis(s, rq.getKey())) {
						System.out.println("Snapshot Synopsis: [ UID: " + s.getSynopsisID() + " | DatasetKey:" + rq.getKey() + " | Digestion timestamp: " + LocalDateTime.now().toString() + " ]");
						snapshotFlag = true;
					}else{
						// The snapshot failed due to storage error
						System.out.println("Snapshot Synopsis Failed: [ Internal Storage Error ] ");
					}
				}
			}
			// If no snapshotting has happened and the Storage Manager is not responsible for the error, then no synopsis was found with matching data
			if(!snapshotFlag){
				System.out.println("Snapshot Synopsis Failed: Could not find synopsis with details: [ UID: "+rq.getUID()+" | DatasetKey: "+rq.getKey()+" ]");
			}
		}


		/**
		 * Request ID:200 --> Load snapshot of synopsis to current running instance of it
		 */
		/**
		 * Request ID:201 --> Load snapshot of synopsis of specified version to current running instance of it
		 */
		else if (rq.getRequestID() == 201 || rq.getRequestID() == 200) {
			// Handle the case for loading CountMin synopsis (ID:1)
			if(rq.getSynopsisID()==1){
				try {
					CountMin loadedObj = null;
					// Handle case where version of desired snapshot is specified by the request parameters
					if(rq.getRequestID() == 201){
						loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), CountMin.class, Integer.valueOf(rq.getParam()[0]));
						// Handle case when the latest version should be loaded
					}else if(rq.getRequestID() == 200){
						loadedObj = StorageManager.loadSynopsisLatestSnapshot(rq.getKey(), rq.getUID(), CountMin.class);
					}

					// Iterator won't work here, because the array is modified during iteration
					// so ConcurrentModificationException will occur.
					for (int i=0; i<M_Synopses.get(rq.getKey()).size(); i++) {
						Synopsis s = M_Synopses.get(rq.getKey()).get(i);
						if (s.getSynopsisID() == rq.getUID()) {
							//Load the version into the Maintenance Array
							Synopses.remove(s);
							Synopses.add(loadedObj);
							//Info print
							String vLoaded = rq.getRequestID()==200 ? String.valueOf(StorageManager.getSynopsisLatestVersionNumber(rq.getUID(), rq.getKey())) : rq.getParam()[0];
							System.out.println("Loaded snapshot of synopsis: [ Version : v" + vLoaded + " --> UID: " + rq.getUID() + " | DatasetKey:" + rq.getKey() + " | Type: CountMin ]");
						}
					}
					// Update the currently maintained synopses
					M_Synopses.put(rq.getKey(), Synopses);
				} catch(Exception e){
					e.printStackTrace();
				}
			}
			// Handle the case for loading BloomFilter synopsis (ID:2)
			if(rq.getSynopsisID()==2){
				try {
					Bloomfilter loadedObj = null;
					// Handle case where version of desired snapshot is specified by the request parameters
					if(rq.getRequestID() == 201){
						loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), Bloomfilter.class, Integer.valueOf(rq.getParam()[0]));
						// Handle case when the latest version should be loaded
					}else if(rq.getRequestID() == 200){
						loadedObj = StorageManager.loadSynopsisLatestSnapshot(rq.getKey(), rq.getUID(), Bloomfilter.class);
					}

					// Iterator won't work here, because the array is modified during iteration
					// so ConcurrentModificationException will occur.
					for (int i=0; i<M_Synopses.get(rq.getKey()).size(); i++) {
						Synopsis s = M_Synopses.get(rq.getKey()).get(i);
						if (s.getSynopsisID() == rq.getUID()) {
							//Load the version into the Maintenance Array
							Synopses.remove(s);
							Synopses.add(loadedObj);
							//Info print
							String vLoaded = rq.getRequestID()==200 ? String.valueOf(StorageManager.getSynopsisLatestVersionNumber(rq.getUID(), rq.getKey())) : rq.getParam()[0];
							System.out.println("Loaded snapshot of synopsis: [ Version : v" + vLoaded + " --> UID: " + rq.getUID() + " | DatasetKey:" + rq.getKey() + " | Type: BloomFilter ]");
						}
					}
					// Update the currently maintained synopses
					M_Synopses.put(rq.getKey(), Synopses);
				} catch(Exception e){
					e.printStackTrace();
				}
			}
			// Handle the case for loading AMSSketch synopsis (ID:3)
			if(rq.getSynopsisID()==3){
				try {
					AMSsynopsis loadedObj = null;
					// Handle case where version of desired snapshot is specified by the request parameters
					if(rq.getRequestID() == 201){
						loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), AMSsynopsis.class, Integer.valueOf(rq.getParam()[0]));
						// Handle case when the latest version should be loaded
					}else if(rq.getRequestID() == 200){
						loadedObj = StorageManager.loadSynopsisLatestSnapshot(rq.getKey(), rq.getUID(), AMSsynopsis.class);
					}

					// Iterator won't work here, because the array is modified during iteration
					// so ConcurrentModificationException will occur.
					for (int i=0; i<M_Synopses.get(rq.getKey()).size(); i++) {
						Synopsis s = M_Synopses.get(rq.getKey()).get(i);
						if (s.getSynopsisID() == rq.getUID()) {
							//Load the version into the Maintenance Array
							Synopses.remove(s);
							Synopses.add(loadedObj);
							//Info print
							String vLoaded = rq.getRequestID()==200 ? String.valueOf(StorageManager.getSynopsisLatestVersionNumber(rq.getUID(), rq.getKey())) : rq.getParam()[0];
							System.out.println("Loaded snapshot of synopsis: [ Version : v" + vLoaded + " --> UID: " + rq.getUID() + " | DatasetKey:" + rq.getKey() + " | Type: AMS Sketch ]");
						}
					}
					// Update the currently maintained synopses
					M_Synopses.put(rq.getKey(), Synopses);
				} catch(Exception e){
					e.printStackTrace();
				}
			}
		}


		/**
		 * Request ID:202 --> Load snapshot of synopsis of specified version to newly instantiated synopsis
		 */
		else if (rq.getRequestID() == 202) {
			// Handle the case for loading CountMin synopsis (ID:1)
			if(rq.getSynopsisID()==1){
				try {
					CountMin loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), CountMin.class, Integer.valueOf(rq.getParam()[0]));
					loadedObj.setSynopsisID(Integer.valueOf(rq.getParam()[1]));
					Synopses.add(loadedObj);
					System.out.println("Loaded snapshot of synopsis into new instance: [ Version : v" + rq.getParam()[0] + " | New UID: " + rq.getParam()[1] + " | DatasetKey:" + rq.getKey() + " | Type: CountMin ]");
					M_Synopses.put(rq.getKey(), Synopses);
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			// Handle the case for loading BloomFilter synopsis (ID:2)
			if(rq.getSynopsisID()==2){
				try {
					Bloomfilter loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), Bloomfilter.class, Integer.valueOf(rq.getParam()[0]));
					loadedObj.setSynopsisID(Integer.valueOf(rq.getParam()[1]));
					Synopses.add(loadedObj);
					System.out.println("Loaded snapshot of synopsis into new instance: [ Version : v" + rq.getParam()[0] + " | New UID: " + rq.getParam()[1] + " | DatasetKey:" + rq.getKey() + " | Type: BloomFilter ]");
					M_Synopses.put(rq.getKey(), Synopses);
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			// Handle the case for loading AMSSketch synopsis (ID:3)
			if(rq.getSynopsisID()==3){
				try {
					AMSsynopsis loadedObj = StorageManager.loadSynopsisSnapshot(rq.getKey(), rq.getUID(), AMSsynopsis.class, Integer.valueOf(rq.getParam()[0]));
					loadedObj.setSynopsisID(Integer.valueOf(rq.getParam()[1]));
					Synopses.add(loadedObj);
					System.out.println("Loaded snapshot of synopsis into new instance: [ Version : v" + rq.getParam()[0] + " | New UID: " + rq.getParam()[1] + " | DatasetKey:" + rq.getKey() + " | Type: AMS Sketch ]");
					M_Synopses.put(rq.getKey(), Synopses);
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		}

		/**
		 * Debug request that prints out the state of a running synopsis based on the request parameters
		 */
		else if(rq.getRequestID() == 1111){
			try {
				for (Synopsis s : M_Synopses.get(rq.getKey())) {
					if(s.getSynopsisID()==rq.getUID()){
						if(s instanceof AMSsynopsis){
							System.out.println("[ Synopsis type: "+s.getClass().getSimpleName()+" | UID: "+ s.getSynopsisID() + " | DatasetKey: "+rq.getKey()+ " ]\n--> Synopsis State:\n"+((AMSsynopsis) s).toJson());
						}
					}
				}
			}catch (Exception e){
				e.printStackTrace();
			}
		}
		else if(rq.getRequestID() == 1000){
			try{
				StringBuilder builder = new StringBuilder();
				for(String datasetKey : M_Synopses.keySet()){
					builder.append("[ Synopses maintained for Key: "+datasetKey+ " ]");
					builder.append("\n");
					for(Synopsis s: M_Synopses.get(datasetKey)){
						builder.append("\t Synopsis: [ UID: "+s.getSynopsisID()+ " | Dataset Key: "+datasetKey+ " | Type: "+s.getClass().getSimpleName()+ " ]");
						builder.append("\n");
					}
				}
				System.out.println(builder.toString());
			}catch(Exception e){
				e.printStackTrace();
			}

		}
		/**
		 * Request ID:1 --> Add synopsis with keyed partitioning (non continuous)
		 * Request ID:4 --> Add synopsis with random partitioning (non continuous)
		 */
		else if (rq.getRequestID() == 1 || rq.getRequestID() == 4 ) {

			if(Synopses==null){
				Synopses = new ArrayList<>();
			}

			Synopsis newSketch = null;

			//Check what type of synopsis the request asks for and create it
			switch (rq.getSynopsisID()) {
				// countMin
				case 1:
					if (rq.getParam().length > 4) {
						newSketch = new CountMin(rq.getUID(), rq.getParam());
						System.out.println("[INFO] Maintaining new CountMin synopsis [Type ID: "+rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()+"] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for CountMin synopsis, will not add new instance");
					}
					//{ "1", "2", "0.0002", "0.99", "4" };
					Synopses.add(newSketch);
				break;
				// BloomFliter
				case 2:
					if (rq.getParam().length > 3) {
						newSketch = new Bloomfilter(rq.getUID(), rq.getParam());
						System.out.println("[INFO] Maintaining new BloomFilter synopsis [Type ID: "+rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()+"] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for BloomFilter synopsis, will not add new instance");
					}
					//	String[] _tmp = { "1", "1", "100000", "0.0002" };
					Synopses.add(newSketch);
				break;
				// AMS Sketch
				case 3:
					if (rq.getParam().length > 3){
						newSketch = new AMSsynopsis(rq.getUID(), rq.getParam());
						System.out.println("[INFO] Maintaining new AMSketch synopsis [Type ID: "+rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()+"] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for AMSketch synopsis, will not add new instance");
					}
					//	String[] _tmp = { "1", "2", "1000", "10" };
					Synopses.add(newSketch);
				break;
				// DFT
				case 4:
					if (rq.getParam().length > 3){
						newSketch = new MultySynopsisDFT(rq.getUID(), rq.getParam());
						System.out.println("[INFO] Maintaining new DFT synopsis [Type ID: "+rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()+"] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for DFT synopsis, will not add new instance");
					}
					//String[] _tmp = {"1", "2", "5", "30", "8"};
					Synopses.add(newSketch);
				break;
				//LSH - undone, replaced with BloomFilter
				case 5:
					newSketch = new Bloomfilter(rq.getUID(), rq.getParam());
					Synopses.add(newSketch);

				break;
				// lib.Coresets
				case 6:
					if (rq.getParam().length > 10 ){
						newSketch = new FinJoinCoresets(rq.getUID(), rq.getParam());
						System.out.println("[INFO] Maintaining new Coresets synopsis [Type ID: "+rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()+"] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for Coresets synopsis, will not add new instance");
					}
					//	String[] _tmp = { "1","2", "5", "10" };
					Synopses.add(newSketch);
				break;
				// HyperLogLog
				case 7:
					if (rq.getParam().length > 2){
						newSketch = new HyperLogLogSynopsis(rq.getUID(), rq.getParam());
						System.out.println("[INFO] Maintaining new HyperLogLog synopsis [Type ID: "+rq.getSynopsisID()+" | StreamID: "+rq.getStreamID()+" | DatasetKey: "+rq.getKey()+"] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for HyperLogLog synopsis, will not add new instance");
					}
					//String[] _tmp = { "1", "1", "0.001" };
					Synopses.add(newSketch);
				break;
				// StickySampling
				case 8:

					if (rq.getParam().length > 4) {
						newSketch = new StickySamplingSynopsis(rq.getUID(), rq.getParam());
						System.out.println("[INFO] Maintaining new StickySampling synopsis [Type ID: " + rq.getSynopsisID() + " | StreamID: " + rq.getStreamID() + " | DatasetKey: " + rq.getKey() + "] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for StickySampling synopsis, will not add new instance");
					}
					//String[] _tmp = { "1", "2", "0.01", "0.01", "0.0001"};
					Synopses.add(newSketch);
				break;
				// LossyCounting
				case 9:

					if (rq.getParam().length > 2){
						newSketch = new LossyCountingSynopsis(rq.getUID(), rq.getParam());
					    System.out.println("[INFO] Maintaining new LossyCounting synopsis [Type ID: " + rq.getSynopsisID() + " | StreamID: " + rq.getStreamID() + " | DatasetKey: " + rq.getKey() + "] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for LossyCounting synopsis, will not add new instance");
					}
					//String[] _tmp = { "1", "2", "0.0001" };

					Synopses.add(newSketch);
				break;
				// ChainSampler
				case 10:

					if (rq.getParam().length > 3){
						newSketch = new ChainSamplerSynopsis(rq.getUID(), rq.getParam());
						System.out.println("[INFO] Maintaining new ChainSampler synopsis [Type ID: " + rq.getSynopsisID() + " | StreamID: " + rq.getStreamID() + " | DatasetKey: " + rq.getKey() + "] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for ChainSampler synopsis, will not add new instance");
					}
					//String[] _tmp = { "2", "2", "1000", "100000" };
					Synopses.add(newSketch);
				break;
				// GKQuantiles
				case 11:

					if (rq.getParam().length > 3){
						newSketch = new GKsynopsis(rq.getUID(), rq.getParam());
						System.out.println("[INFO] Maintaining new GK synopsis [Type ID: " + rq.getSynopsisID() + " | StreamID: " + rq.getStreamID() + " | DatasetKey: " + rq.getKey() + "] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for GK synopsis, will not add new instance");
					}
					//String[] _tmp = { "2", "2", "0.01"};
					Synopses.add(newSketch);
				break;
				// lib.TopK
				case 13:
					if (rq.getParam().length > 3){
						newSketch = new SynopsisTopK(rq.getUID(), rq.getParam());
					    System.out.println("[INFO] Maintaining new Top-K synopsis [Type ID: " + rq.getSynopsisID() + " | StreamID: " + rq.getStreamID() + " | DatasetKey: " + rq.getKey() + "] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for Top-K synopsis, will not add new instance");
					}
					//String[] _tmp = { "2", "2", "0.01"};
					Synopses.add(newSketch);
				break;
				// windowQuantiles
				case 16:
					if (rq.getParam().length > 3){
						newSketch = new windowQuantiles(rq.getUID(), rq.getParam());
					    System.out.println("[INFO] Maintaining new WindowQuantiles synopsis [Type ID: " + rq.getSynopsisID() + " | StreamID: " + rq.getStreamID() + " | DatasetKey: " + rq.getKey() + "] upon request: " + rq.getUID());
					} else{
						System.out.println("[ERROR] Insufficient parameters for WindowQuantiles synopsis, will not add new instance");
					}
					//String[] _tmp = { "2", "2", "0.01"};
					Synopses.add(newSketch);
				break;
				// 25-> dynamic load newSketch
				case 25:

					Object instance;

					if (rq.getParam().length == 4) {

						File myJar = new File(rq.getParam()[2]);
						URLClassLoader child = new URLClassLoader(new URL[]{myJar.toURI().toURL()},
						this.getClass().getClassLoader());
						Class<?> classToLoad = Class.forName(rq.getParam()[3], true, child);
						instance = classToLoad.getConstructor().newInstance();
						Synopses.add((Synopsis) instance);

					} else {

						File myJar = new File("C:\\Users\\ado.kontax\\Desktop\\flinkSketches.jar");
						URLClassLoader child = new URLClassLoader(new URL[]{myJar.toURI().toURL()},
						this.getClass().getClassLoader());
						Class<?> classToLoad = Class.forName("com.yahoo.sketches.sampling.NewSketch", true, child);
						instance = classToLoad.getConstructor().newInstance();
						Synopses.add((Synopsis) instance);

					}
				break;
				// FINJOIN
				case 26:

					if (rq.getParam().length > 3)
						newSketch = new FinJoinSynopsis(rq.getUID(), rq.getParam());
					//String[] _tmp = { "0", "0", "10", "100", "8", "3" };
					Synopses.add(newSketch);

				break;
				// COUNT
				case 27:

					if (rq.getParam().length > 3)
						newSketch = new Counters(rq.getUID(), rq.getParam());
					else {
						String[] _tmp = {"0", "0", "10", "100", "8", "3"};
						newSketch = new Counters(rq.getUID(), _tmp);
					}
					Synopses.add(newSketch);
				break;
				//window lsh
				case 28:
					System.out.println("ADD-> _ " +rq.toString());
					if (rq.getParam().length > 3)
						newSketch = new WLSHSynopses(rq.getUID(), rq.getParam());

					Synopses.add(newSketch);
				break;
				//window pastDFT
				case 29:
					System.out.println("ADD-> _ " +rq.toString());
					if (rq.getParam().length > 3)
						newSketch = new PastDFTSynopsis(rq.getUID(), rq.getParam());
					Synopses.add(newSketch);
					break;
			}
			M_Synopses.put(rq.getKey(),Synopses);
		}


		/**
		 * Request ID:5 --> Add synopsis with keyed partitioning (continuous)
		 *
		 */
		else if(rq.getRequestID() == 5) {
			if (C_Synopses == null){
				C_Synopses = new ArrayList<>();
			}
			ContinuousSynopsis sketch = null;

			switch (rq.getSynopsisID()) {

				// ContinuousCM
				case 1:
					if (rq.getParam().length > 4)
						sketch = new ContinuousCM(rq.getUID(), rq, rq.getParam());
						//String[] _tmp = { "StockID", "Volume", "0.0002", "0.99", "4" };
						C_Synopses.add(sketch);
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;
				// RadiusSketch
				case 100:
					if (rq.getParam().length > 4)
						sketch = new Radius_Grid(rq);
					C_Synopses.add(sketch);
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;
				//MaritimeSketch
				case 12:
					rq.setNoOfP(1);
					if (rq.getParam().length > 5){
						sketch = new ContinuousMaritimeSketches(rq.getUID(), rq, rq.getParam());
					        System.out.println("[INFO] Maintaining new ContinuousMaritimeSketch synopsis [Type ID: " + rq.getSynopsisID() + " | StreamID: " + rq.getStreamID() + " | DatasetKey: " + rq.getKey() + "] upon request: " + rq.getUID());
						} else{
							System.out.println("[ERROR] Insufficient parameters for ContinuousMaritimeSketch synopsis, will not add new instance");
						}
					//String[] _tmp = {"1", "1", "18000","10000","50","50"};
					C_Synopses.add(sketch);
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;
				case 15:
					if (rq.getParam().length > 5)
						sketch = new ISWoR(rq.getUID(), rq, rq.getParam());
					//String[] _tmp = {"1", "1", "18000","10000","50","50"};
					C_Synopses.add(sketch);
					MC_Synopses.put(rq.getKey(), C_Synopses);
					break;

			}
		}
		/**
		 * The 'else' clause below handles the following request cases:
		 *
		 * Request ID: 2 --> Delete a currently maintained synopsis based on ID
		 * Request ID: 3 --> Estimate a queryable synopsis
		 * Request ID: 6 --> Estimate in an advanced way a queryable synopsis
		 * // TODO Update synopsis state request (e.g. latest v restore, restore to snapshot etc.)
		 * Request ID: 7 --> Update the state of a maintained synopsis (Not handled everywhere, could take advantage of it)
		 *
		 */
		else {
			if(Synopses==null){
				System.out.println("No synopsis found for Data Set Key: "+rq.getKey()+" ! Please maintain a synopsis before querying");
			}else {
				for (Synopsis syn : Synopses) {

					if (rq.getUID() == syn.getSynopsisID()) {

						//Remove synopsis request handling.
						if (rq.getRequestID() % 10 == 2) {
							Synopses.remove(syn);
							M_Synopses.put(rq.getKey(), Synopses);
							System.out.println("Synopsis of Type "+rq.getSynopsisID()+" with UID: "+rq.getUID()+" for DatasetKey:"+rq.getKey()+" and StreamID: "+rq.getStreamID()+" has been deleted");

						} else if ((rq.getRequestID() % 10 == 3) || (rq.getRequestID() % 10 == 6)) {

							Estimation e = syn.estimate(rq);
							if (e.getEstimation() != null) {
								if (rq.getSynopsisID() == 28) {

									HashMap<Integer, Bucket> buckets = (HashMap<Integer, Bucket>) e.getEstimation();

									for (Map.Entry<Integer, Bucket> entry : buckets.entrySet()) {
										Integer key = entry.getKey();
										Bucket value = entry.getValue();
										System.out.println("Bucket No. -> " + key + "Pid:" + pId + "\n INFO -> " + value.toString());
										e.setKey(e.getUID() + "_" + key);
										e.setEstimationkey(e.getUID() + "_" + key + "_" + pId);
										e.setEstimation(value);
										//Estimation e1 = new Estimation(e);
										collector.collect(e);

									}
								} else if (rq.getSynopsisID() == 29) {

									HashMap<String, controlBucket> buckets = (HashMap<String, controlBucket>) e.getEstimation();

									for (Map.Entry<String, controlBucket> entry : buckets.entrySet()) {

										String key = entry.getKey();
										//System.out.println("Keys -> " + key);
										controlBucket value = entry.getValue();
										if (value != null)
											System.out.println("Bucket BEFORE with KEY ->" + key + " INFO -> " + value.toString());
										e.setKey(key);
										e.setEstimationkey(e.getUID() + "_" + key + "_" + pId);
										e.setEstimation(value);
										//System.out.println(e.toString());
										collector.collect(e);
									}
								} else {
									collector.collect(e);
								}
							}

						}
					}

				}
			}
		}
	}

	public void open(Configuration config)  {
	 	pId = getRuntimeContext().getIndexOfThisSubtask();
	}

}
