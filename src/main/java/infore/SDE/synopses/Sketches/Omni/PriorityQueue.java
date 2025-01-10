package infore.SDE.synopses.Sketches.Omni;

// Java code to implement
// priority-queue using
// array implementation of
// binary heap

public class PriorityQueue {

    int[] H;
    public int size = -1;
    public int K;

    public PriorityQueue(int size) {
        this.K = size;
        H = new int[size];
        for (int i = 0; i < size; i++) {
            H[i] = -1;
        }
    }

//    public PriorityQueue(PriorityQueue priorityQueue) {
//        this.H = priorityQueue.H;
//        this.size = priorityQueue.size;
//        this.K = priorityQueue.K;
//    }

    // Function to return the index of the
    // parent node of a given node
    static int parent(int i) {
        return (i - 1) / 2;
    }

    public static PriorityQueue getCopy(PriorityQueue priorityQueue) {
        PriorityQueue copy = new PriorityQueue(priorityQueue.K);
        copy.size = priorityQueue.size;
        if (priorityQueue.size >= 0) System.arraycopy(priorityQueue.H, 0, copy.H, 0, priorityQueue.size);
        return copy;
    }

    // Function to return the index of the
    // left child of the given node
    static int leftChild(int i) {
        return ((2 * i) + 1);
    }

    // Function to return the index of the
    // right child of the given node
    static int rightChild(int i) {
        return ((2 * i) + 2);
    }

    // Function to shift up the
    // node in order to maintain
    // the heap property
    void shiftUp(int i) {
        while (i > 0 &&
                this.H[parent(i)] < this.H[i]) {
            // Swap parent and current node
            swap(parent(i), i);

            // Update i to parent of i
            i = parent(i);
        }
    }

    // Function to shift down the node in
    // order to maintain the heap property
    void shiftDown(int i) {
        int maxIndex = i;

        // Left Child
        int l = leftChild(i);

        if (l <= size &&
                H[l] > H[maxIndex]) {
            maxIndex = l;
        }

        // Right Child
        int r = rightChild(i);

        if (r <= size &&
                H[r] > H[maxIndex]) {
            maxIndex = r;
        }

        // If i not same as maxIndex
        if (i != maxIndex) {
            swap(i, maxIndex);
            shiftDown(maxIndex);
        }
    }

    // Function to insert a
    // new element in
    // the Binary Heap
    void insert(int p) {
        size = size + 1;
        if (size >= K) {
            throw new RuntimeException("Size exceeded");
        }
        H[size] = p;

        // Shift Up to maintain
        // heap property
        shiftUp(size);
    }

    // Function to extract
    // the element with
    // maximum priority
    void extractMax() {
        int result = H[0];

        // Replace the value
        // at the root with
        // the last leaf
        H[0] = H[size];
        size = size - 1;
//        H[size + 1] = -1;

        // Shift down the replaced
        // element to maintain the
        // heap property
        shiftDown(0);
    }

    // Function to change the priority
// of an element
    void changePriority(int i,
                        int p) {
        int oldp = H[i];
        H[i] = p;

        if (p > oldp) {
            shiftUp(i);
        } else {
            shiftDown(i);
        }
    }

    // Function to get value of
// the current maximum element
    int getMax() {
        return H[0];
    }

    // Function to remove the element
// located at given index
    void remove(int i) {
        H[i] = getMax() + 1;

        // Shift the node to the root
        // of the heap
        shiftUp(i);

        // Extract the node
        extractMax();
    }

    void swap(int i, int j) {
        int temp = H[i];
        H[i] = H[j];
        H[j] = temp;
    }

    public boolean isEmpty() {
        // check if the priority queue is empty
        return size == -1;
    }

    public int peek() {
        // return the element with the highest priority
        return getMax();
    }

    public void poll() {
        // remove the element with the highest priority
        extractMax();
    }

    public boolean add(int hx) {
        // add an element to the priority queue
        insert(hx);
        return true;
    }

    public void reset() {
        H = new int[0];
        size = -1;
    }
}
