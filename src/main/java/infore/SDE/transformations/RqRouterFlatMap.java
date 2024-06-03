package infore.SDE.transformations;

import java.io.Serializable;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import infore.SDE.messages.Request;

public class RqRouterFlatMap extends RichFlatMapFunction<Request, Request> implements Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Request rq, Collector<Request> out) throws Exception {
		if (rq.getRequestID() % 10 >= 8) {
			return;
		}

		String tmpkey = rq.getKey();

		if (rq.getNoOfP() == 1) {
			out.collect(rq);
			return;
		}

		if (rq.getRequestID() % 10 == 6) {
			handleRequestID6(rq, tmpkey, out);
		} else if (rq.getSynopsisID() == 100) {
			handleSynopsisID100(rq, tmpkey, out);
		} else {
			handleDefaultCase(rq, tmpkey, out);
		}
	}

	private void handleRequestID6(Request rq, String tmpkey, Collector<Request> out) {
		int n = Integer.parseInt(rq.getParam()[0]);
		String[] dataSets = rq.getDataSetkey().split(",");
		String[] params = rq.getParam();
		params[1] = String.valueOf(rq.getUID());

		for (int i = 0; i < n; i++) {
			rq.setUID(Integer.parseInt(params[i + 1]));
			for (int j = 0; j < rq.getNoOfP(); j++) {
				rq.setKey(dataSets[i] + "_" + rq.getNoOfP() + "_KEYED_" + j);
				out.collect(rq);
			}
		}
	}

	private void handleSynopsisID100(Request rq, String tmpkey, Collector<Request> out) {
		int iterations = Integer.parseInt(rq.getParam()[2]);
		for (int i = 0; i < iterations; i++) {
			rq.setDataSetkey(tmpkey + "_" + i);
			out.collect(rq);
		}
	}

	private void handleDefaultCase(Request rq, String tmpkey, Collector<Request> out) {
		for (int i = 0; i < rq.getNoOfP(); i++) {
			if (rq.getRequestID() % 10 == 4) {
				rq.setKey(tmpkey + "_" + rq.getNoOfP() + "_RANDOM_" + i);
			} else {
				rq.setDataSetkey(tmpkey + "_" + rq.getNoOfP() + "_KEYED_" + i);
			}
			out.collect(rq);
		}
	}
}
