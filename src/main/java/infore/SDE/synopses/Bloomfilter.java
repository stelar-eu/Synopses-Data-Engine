package infore.SDE.synopses;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;

import java.io.*;

public class Bloomfilter extends Synopsis implements Serializable {
	private static final long serialVersionUID = 1L;
	private transient BloomFilter bm;

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();

		try {
			// Serialize BloomFilter fields
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			ObjectOutputStream objStream = new ObjectOutputStream(byteStream);
			objStream.writeObject(bm);
			objStream.flush();

			// Write serialized BloomFilter bytes to output stream
			byte[] bloomFilterBytes = byteStream.toByteArray();
			oos.writeObject(bloomFilterBytes);
		} catch (IOException e) {
			throw new IOException("Failed to serialize BloomFilter", e);
		}
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();

		try {
			// Read serialized BloomFilter bytes from input stream
			byte[] bloomFilterBytes = (byte[]) ois.readObject();

			// Deserialize BloomFilter
			ByteArrayInputStream byteStream = new ByteArrayInputStream(bloomFilterBytes);
			ObjectInputStream objStream = new ObjectInputStream(byteStream);
			bm = (BloomFilter) objStream.readObject();
		} catch (IOException | ClassNotFoundException e) {
			throw new IOException("Failed to deserialize BloomFilter", e);
		}
	}

	public Bloomfilter(int uid, String[] parameters) {
		super(uid, parameters[0], parameters[1], parameters[2]);
		bm = new BloomFilter(Integer.parseInt(parameters[3]), Double.parseDouble(parameters[4]));
	}

	@Override
	public void add(Object k) {
		JsonNode node = (JsonNode) k;
		String key = node.get(this.keyIndex).asText();
		bm.add(key);
	}

	@Override
	public String estimate(Object k) {
		if (bm.isPresent((String) k)) {
			return "1";
		}
		return "0";
	}

	@Override
	public Estimation estimate(Request rq) {
		return new Estimation(rq, bm.isPresent(rq.getParam()[0]), Integer.toString(rq.getUID()));
	}

	@Override
	public Synopsis merge(Synopsis sk) {
		if (sk instanceof Bloomfilter) {
			Bloomfilter other = (Bloomfilter) sk;
			this.bm.addAll(other.bm);
			return this;
		}
		return null;
	}
}
