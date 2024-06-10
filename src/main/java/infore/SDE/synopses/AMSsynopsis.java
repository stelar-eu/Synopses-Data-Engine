package infore.SDE.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import org.streaminer.stream.frequency.AMSSketch;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.*;
import java.lang.reflect.Field;

public class AMSsynopsis extends Synopsis implements Serializable {
	private static final long serialVersionUID = 1L;
	private transient AMSSketch ams;

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
