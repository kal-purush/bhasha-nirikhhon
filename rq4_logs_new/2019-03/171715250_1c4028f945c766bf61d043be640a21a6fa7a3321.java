package org.team5.app.main;

import org.team5.app.dataprocessing.CSVReader;
import org.team5.app.dataprocessing.DataPoint;

import java.util.concurrent.BlockingQueue;

public class InputThread implements Runnable {

    private String csvFilePath;
    private BlockingQueue<DataPoint> buffer;

    /**
     * @param buffer the blocking queue buffer that holds message rates per time
     * @param csvFilePath path to the csv file to be read
     */
    public InputThread(BlockingQueue<DataPoint> buffer, String csvFilePath) {
        this.csvFilePath = csvFilePath;
        this.buffer = buffer;
    }

    /**
     * This thread reads the message rates into a blocking queue buffer.
     * A BlockingQueue is a queue that additionally supports operations that wait for
     * the queue to become non-empty when retrieving an element, and wait for space to become available
     * in the queue when storing an element.
     * <p>
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     */
    @Override
    public void run() {
        System.out.println("InputThread running");

        CSVReader reader = new CSVReader(csvFilePath);

        for (int i = 0; i < reader.dataPoints.size(); i++) {

            //Start queueing up the message rate in a separate thread
            try {
                buffer.put(reader.dataPoints.get(i));
                System.out.println("Put message rate "+reader.dataPoints.get(i).getValue());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("InputThread exiting");
    }

    public void checkSize() {
        System.out.println("Buffer size: " + buffer.size());
    }

    public DataPoint remove() {
        try {
            return buffer.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}