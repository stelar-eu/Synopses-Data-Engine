package infore.SDE.synopses.Sketches.Omni;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.Collections;
//import java.util.PriorityQueue;
import infore.SDE.synopses.Sketches.Omni.PriorityQueue;
import java.util.logging.Logger;

// Min-wise hashing as described in paper by Rasmus Pagh: https://dl.acm.org/doi/10.1145/2594538.2594554

public class Minwise {


    public PriorityQueue sketch;

    private final int K;
    private final int b;
    private final int seed;
    HashFunction xx;
    int maxHash;
    public int curSampleSize = 0;
    public int n = 0;

    public long curTreeRoot = Long.MAX_VALUE;



    public int hash(String x) {
        // charset is not specified, so we use the default charset. Change if necessary.
        int hash = xx.hashString(x, Charset.defaultCharset()).asInt() % maxHash;
        if (hash < 0) {
            hash = hash + maxHash;
        }
        return hash;
    }

    public Minwise(int maxSize, int b, int seed) {
        this.K = maxSize;
        this.b = b;
        this.seed = seed;
        maxSize = (int) Math.pow(2, b) - 1;
        this.sketch = new PriorityQueue(K);
        xx = Hashing.murmur3_32_fixed(seed);
    }


    public void ingest(int hx, int sign) {
        if (sign == 1) {
            add(hx);
        } else {
            remove(hx);
        }
    }


    public void remove(int hx) {
        Logger.getGlobal().warning("Remove not implemented for Minwise / OmniSketch");
    }

    public void add(int hx) {
        n++;
        if (curSampleSize < K - 1) {
            sketch.add(hx);
            curSampleSize++;
        } else if (curSampleSize == K) {
            curTreeRoot = sketch.peek();
            if (hx < curTreeRoot) { // get tree root
                sketch.poll();
                //sketch.pollFirst();
                if (sketch.add(hx)) {
                    curTreeRoot = sketch.peek();
                } else {
                    curTreeRoot = sketch.peek();
                }
            }
        }
        // check if hx is in the sketch already, if so, do nothing
        else {
            // Only proceed if hx is smaller than the current root (curTreeRoot)
            if (hx < curTreeRoot) {
                // Try to add hx directly, avoid a separate contains() check
                if (sketch.add(hx)) {  // Add hx to the set, returns false if already present
                    sketch.poll();  // Remove the smallest element (previous curTreeRoot)
                    curTreeRoot = sketch.peek();  // Update curTreeRoot to the new smallest element
                }
            }
        }
    }

    public void reset() {
        sketch.reset();
        this.curSampleSize = 0;
    }

    public PriorityQueue getSampleToQuery() {
        return PriorityQueue.getCopy(sketch);
    }



}
