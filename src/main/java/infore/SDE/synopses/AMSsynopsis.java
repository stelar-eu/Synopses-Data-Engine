package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import org.streaminer.stream.frequency.AMSSketch;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.*;
import java.lang.reflect.Field;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.HashMap;
import java.util.Map;

public class AMSsynopsis extends Synopsis implements Serializable {
	private static final long serialVersionUID = 1L;
	private transient AMSSketch ams;

	/**
	 * 	Serializable implemented method for writing an AMSSynopsis in a
	 * 	serial form into object key-value store.	 *
	 * @param oos The Object Output Stream
	 * @throws IOException
	 */
	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();

		try {
			// Access private fields using reflection
			Field depthField = AMSSketch.class.getDeclaredField("depth");
			Field bucketsField = AMSSketch.class.getDeclaredField("buckets");
			Field countField = AMSSketch.class.getDeclaredField("count");
			Field countsField = AMSSketch.class.getDeclaredField("counts");
			Field testField = AMSSketch.class.getDeclaredField("test");

			// Make private fields accessible
			depthField.setAccessible(true);
			bucketsField.setAccessible(true);
			countField.setAccessible(true);
			countsField.setAccessible(true);
			testField.setAccessible(true);

			// Write fields to output stream
			oos.writeInt(depthField.getInt(ams));
			oos.writeInt(bucketsField.getInt(ams));
			oos.writeInt(countField.getInt(ams));
			oos.writeObject(countsField.get(ams));
			oos.writeObject(testField.get(ams));
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new IOException("Failed to serialize AMSSketch", e);
		}
	}
	/**
	 * 	Serializable implemented method for reading an AMSSynopsis
	 * 	originating from a generic object store and creating a Java
	 * 	AMSSynopsis object.
	 * @param ois The Object Input Stream reading the serialized object
	 * @throws IOException
	 */
	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();

		try {
			int depth = ois.readInt();
			int buckets = ois.readInt();
			int count = ois.readInt();
			int[] counts = (int[]) ois.readObject();
			long[][] test = (long[][]) ois.readObject();

			ams = new AMSSketch(depth, buckets);

			// Access private fields using reflection
			Field countField = AMSSketch.class.getDeclaredField("count");
			Field countsField = AMSSketch.class.getDeclaredField("counts");
			Field testField = AMSSketch.class.getDeclaredField("test");

			// Make private fields accessible
			countField.setAccessible(true);
			countsField.setAccessible(true);
			testField.setAccessible(true);

			// Set fields to the deserialized values
			countField.setInt(ams, count);
			countsField.set(ams, counts);
			testField.set(ams, test);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new IOException("Failed to deserialize AMSSketch", e);
		}
	}

	/**
	 * 	Return the synopsis in auniversal format (JSON here). Used
	 * 	mainly for storing service-friendly formats of the synopsis
	 * 	along the serialization
	 *  @throws IOException
	 */
	public String toJson() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		mapper.enable(SerializationFeature.INDENT_OUTPUT);  // Enable pretty printing

		// Serialize parent class fields
		ObjectNode jsonNode = mapper.createObjectNode();
		jsonNode.put("SynopsisID", this.SynopsisID);
		jsonNode.put("keyIndex", this.keyIndex);
		jsonNode.put("valueIndex", this.valueIndex);
		jsonNode.put("operationMode", this.operationMode);

		try {
			// Access private fields using reflection
			Field depthField = AMSSketch.class.getDeclaredField("depth");
			Field bucketsField = AMSSketch.class.getDeclaredField("buckets");
			Field countField = AMSSketch.class.getDeclaredField("count");
			Field countsField = AMSSketch.class.getDeclaredField("counts");
			Field testField = AMSSketch.class.getDeclaredField("test");

			// Make private fields accessible
			depthField.setAccessible(true);
			bucketsField.setAccessible(true);
			countField.setAccessible(true);
			countsField.setAccessible(true);
			testField.setAccessible(true);

			// Prepare fields for JSON serialization
			Map<String, Object> sketchData = new HashMap<>();
			sketchData.put("depth", depthField.getInt(ams));
			sketchData.put("buckets", bucketsField.getInt(ams));
			sketchData.put("count", countField.getInt(ams));
			sketchData.put("counts", countsField.get(ams));
			sketchData.put("test", testField.get(ams));

			// Convert sketchData to JSON and add to parent JSON node
			ObjectNode amsNode = mapper.convertValue(sketchData, ObjectNode.class);
			jsonNode.set("ams", amsNode);

			return mapper.writeValueAsString(jsonNode);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new IOException("Failed to serialize AMSSketch", e);
		}
	}

	public AMSSketch getAMS() {
		return ams;
	}

	public AMSsynopsis() {
		super();
	}
	public AMSsynopsis(int uid, String[] parameters) {
		super(uid, parameters[0], parameters[1], parameters[2]);
		ams = new AMSSketch(Integer.parseInt(parameters[3]), Integer.parseInt(parameters[4]));
	}

	@Override
	public void add(Object k) {
		JsonNode node = (JsonNode) k;
		String key = node.get(this.keyIndex).asText();
		String value = node.get(this.valueIndex).asText();
		ams.add((long) Math.abs(key.hashCode()), (long) Double.parseDouble(value));
	}

	@Override
	public String estimate(Object k) {
		return Long.toString(ams.estimateCount((long) k));
	}

	@Override
	public Synopsis merge(Synopsis sk) {
		if (sk instanceof AMSsynopsis) {
			AMSsynopsis other = (AMSsynopsis) sk;
			this.ams.add(other.getAMS());
			return this;
		}
		return null;
	}

	@Override
	public Estimation estimate(Request rq) {
		try {
			return new Estimation(rq, Double.toString((double) ams.estimateCount((long) Math.abs(rq.getParam()[0].hashCode()))), Integer.toString(rq.getUID()));
		} catch (Exception e) {
			return new Estimation(rq, null, Integer.toString(rq.getUID()));
		}
	}
}
