package infore.SDE.synopses.Sketches.Omni;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.nio.ByteBuffer;
import java.util.Random;

public class OmniAttributeSketch {

    //ArrayList<ArrayList<Sample>> CM = new ArrayList<>();


    public Minwise[][] AttrSketch;
    final int depth;
    final int width;
    final int orgMaxSize;
    final int b;
    final int seed;
//    public static XXHash64 hashFunc = XXHashFactory.fastestInstance().hash64();

    int insertCount = 0;

    HashFunction xx = Hashing.murmur3_32_fixed();
    public OmniAttributeSketch(int depth, int width, int B, int b,  int seed){
        //int depth, int width, int numTwoLHSReps, int B, int b) {
        this.depth = depth;
        this.width = width;
        this.orgMaxSize = B;
        this.b = b;
        this.seed = seed;
        initSketch();
    }


    public void initSketch() {
        AttrSketch = new Minwise[depth][width];
        for (int j = 0; j < depth; j++) {
            for (int i = 0; i < width; i++) {
                AttrSketch[j][i] = new Minwise(orgMaxSize, b, seed);//Main.withDeletes);
            }
        }
    }

    public byte[] longToBytes(long attrValue) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(attrValue);
        return buffer.array();
    }

    int[] hash(long attrValue, int depth, int width) {
        return getHashArray(attrValue, depth, width);
    }

    public void ingest(long attrValue, int sign, int hx) {
        // Test if all element in A and B are consistent
        int[] hashes = hash(attrValue, depth, width);
        insertCount += sign;
        for (int j = 0; j < depth; j++) {
            int w = hashes[j];
            AttrSketch[j][w].ingest(hx, sign); // In Kminwise
        }
    }

    public Minwise[] queryKmin(long attrValue) {
        int[] hashes = hash(attrValue, depth, width);
        Minwise[] result;
        result = new Minwise[depth];

        for (int j = 0; j < depth; j++) {
            int w = hashes[j];
            result[j] = AttrSketch[j][w];
        }
        return result;
    }

    public void reset() {
        for (int j = 0; j < depth; j++) {
            for (int i = 0; i < width; i++) {
                AttrSketch[j][i].reset();
            }
        }
    }


    public int[] getHashArray(final long hash_long, int depth, int width) {
        int[] hashes = new int[depth];
        for (int i = 0; i < depth; i++) {
            xx = Hashing.murmur3_32_fixed(i);
            hashes[i] = ((xx.hashLong(hash_long)).asInt() % width + width) % width;// rn_cm_hash.nextInt(width);
        }
        return hashes;
    }
}