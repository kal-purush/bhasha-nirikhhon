import java.util.List;
import java.util.ArrayList;

public class HeapPriorityQueue<T extends Comparable<T>> {
    public static void main (String[] args) {
    }

    private List<T> storage = new ArrayList<T>();

    private boolean recHeapified(int parentIndex) {
        if (parentIndex > storage.size()) return true;

        int leftChildIndex = parentIndex * 2;
        int rightChildIndex = leftChildIndex + 1;

        if (leftChildIndex <= storage.size() && storage.get(parentIndex-1).compareTo(storage.get(leftChildIndex-1)) > 0) return false;
        if (rightChildIndex <= storage.size() && storage.get(parentIndex-1).compareTo(storage.get(rightChildIndex-1)) > 0) return false;

        return recHeapified(leftChildIndex) && recHeapified(rightChildIndex);
    }

    public boolean heapified() {
        return recHeapified(1);
    }

    public int size() {
        return storage.size();
    }

    private void siftDown(int parentIndex) {
        int leftChildIndex = parentIndex * 2;
        int rightChildIndex = leftChildIndex + 1;

        // base case: parent index is greater than the allocated space so we're done
        if (parentIndex > storage.size()) return;

        // recursive case: value at parent index is greater than either of children so we sift down the smaller of the two
        Integer minIndex = null;

        if (leftChildIndex <= storage.size() && rightChildIndex <= storage.size()) {
            minIndex = storage.get(leftChildIndex-1).compareTo(storage.get(rightChildIndex-1)) < 0 ? leftChildIndex : rightChildIndex;
        } else if (leftChildIndex <= storage.size()) {
            minIndex = leftChildIndex;
        }

        if (minIndex != null && storage.get(parentIndex-1).compareTo(storage.get(minIndex-1)) > 0) {
            swap(parentIndex-1, minIndex-1);
            siftDown(minIndex);
        }
    }

    public T dequeue() {
        // swap the first and last values to get the result in a predicatble place
        swap(0, storage.size()-1);
        T result = storage.remove(storage.size()-1);

        // restore the heap by sifting down
        siftDown(1);

        return result;
    }

    private void swap(int index1, int index2) {
        T temporary = storage.get(index1);
        storage.set(index1, storage.get(index2));
        storage.set(index2, temporary);
    }

    private void heapify(int childIndex) {
        int parentIndex = childIndex/2;

        // base case: if we've reached the root of the heap or the child is greater than or equal to its parent, we're done
        if (childIndex == 1 || storage.get(childIndex-1).compareTo(storage.get(parentIndex-1)) > -1) return;

        // recursive step: swap the child with its parent and heapify one step further
        swap(childIndex-1, parentIndex-1);
        heapify(parentIndex);
    }

    public void enqueue(T element) {
        storage.add(element);
        heapify(storage.size());
    }
}