package org.kingsschools.cyberknights4911.bluetoothpusher;

import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothSocket;
import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.util.Log;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

/**
 * This represents a bluetooth server ready to receive files.
 */
public class ConnectedDevice {
    /**
     * Constants for message types for the internal protocol
     */
    private interface FilePushMessage {
        public static final byte TYPE_FILE = 1;
        public static final byte TYPE_DIRECTORY = 2;
        public static final byte TYPE_STOP = 3;
        public static final byte TYPE_KEEP_ALIVE = 4;
        public static final byte TYPE_FILE_ACK = 5;

        public static final byte COMPRESSION_NONE = 0;
    }


    private static final String TAG = BluetoothPusherService.TAG;

    private BluetoothSocket socket;
    private AtomicBoolean sendingInProgress;
    private AtomicBoolean waitOnFileAck;
    private BluetoothDevice device;
    private InputStream inStream;
    private DataOutputStream outStream;
    private byte[] writeBuffer; // mmBuffer store for the stream
    private byte[] readBuffer; // mmBuffer store for the stream
    private Thread sendingThread;
    private Thread listeningThread;

    private Handler handler;

    public ConnectedDevice(BluetoothDevice device, Handler handler) throws IOException {
        this.device = device;
        this.handler = handler;

        writeBuffer = new byte[1024];
        readBuffer = new byte[1024];

        // only a single file/directory can be sent a time
        sendingInProgress = new AtomicBoolean(false);

        // we use this lock to force the file write to wait until
        // the receiving server has acknowledged the entire file
        // was received.
        waitOnFileAck = new AtomicBoolean(false);
    }

    private void openSocket() {
        try {
            BluetoothSocket socket = device.createRfcommSocketToServiceRecord(UUID.fromString("2d31ac7d-0d4a-48dd-8136-2f6a9b71a3f4"));

            Log.d(TAG, "Connected socket to: " + socket.getRemoteDevice().getName());
            this.socket = socket;
            this.socket.connect();

            // Get the input and output streams; using temp objects because
            // member streams are final.

            try {
                inStream = socket.getInputStream();
            } catch (IOException e) {
                Log.e(TAG, "Error occurred when creating input stream", e);
            }

            try {
                outStream = new DataOutputStream(socket.getOutputStream());
            } catch (IOException e) {
                Log.e(TAG, "Error occurred when creating output stream", e);
            }

            startListening();
        } catch (IOException e) {
            Log.e(TAG, "Error connecting to bluetooth device", e);
        }
    }

    /**
     * Starts the pusher listening for responses from the server.
     * (Currently the server never responds with anything)
     */
    private void startListening() {
        listeningThread = new Thread() {
            public void run() {
                Log.d(TAG, "Listening loop...");
                int numBytes; // bytes returned from read()

                // Keep listening to the InputStream until an exception occurs.
                while (true) {
                    try {
                        // Read from the InputStream.
                        int messageType = inStream.read();
                        Log.d(TAG, "Received Message Type: " + messageType);
                        switch (messageType) {
                            case FilePushMessage.TYPE_KEEP_ALIVE:
                                break;
                            case FilePushMessage.TYPE_FILE_ACK:
                                // let the waiting writeFile() thread continue
                                //mmWaitOnFileAckLock.release();
                                waitOnFileAck.set(false);
                                break;
                        }
                    } catch (IOException e) {
                        Log.d(TAG, "Input stream was disconnected", e);
                        break;
                    }
                }
            }
        };
        listeningThread.start();
    }

    public boolean canSend() {
        return !sendingInProgress.get();
    }

    /**
     * Sends a file or directory to the connected device. On success or failure publishes a message to the
     * handler associated with the pusher.
     */
    public void send(final File path) {
        Log.d(TAG, "Starting to Send File: " + path);
        if (!canSend()) {
            throw new IllegalStateException("A directory is already being sent, cannot send more than one at a time.");
        }

        sendingInProgress.compareAndSet(false, true);
        sendingThread = new Thread() {
            public void run() {
                try {
                    openSocket();
                    Log.d(TAG, "Sending File: " + path);
                    if (path.isDirectory()) {
                        writeDirectory(path, new File("/"));
                    } else {
                        writeFile(path, null);
                    }
                    sendSuccessMessage(path);
                } catch (IOException e) {
                    Log.e(TAG, "Error occurred when sending data", e);
                    sendFailureMessage(path);
                } finally {
                    disconnect();
                }
            }
        };
        sendingThread.start();
    }

    // TODO Use the file name in the message
    // TODO Get rid of the readBuffer and use a dedicated buffer for message sending...if needed
    private void sendSuccessMessage(File file) {
        // Share the sent message with the UI activity.
        Message msg = handler.obtainMessage(BluetoothPusherService.StatusMessageTypes.SEND_SUCCESS);
        Bundle bundle = new Bundle();
        bundle.putString("file", file.getName());
        msg.setData(bundle);
        msg.sendToTarget();
    }

    // TODO Use the file name in the message
    // TODO Get rid of the readBuffer and use a dedicated buffer for message sending...if needed
    private void sendFailureMessage(File file) {
        // Share the sent message with the UI activity.
        Message msg = handler.obtainMessage(BluetoothPusherService.StatusMessageTypes.SEND_FAILED);
        Bundle bundle = new Bundle();
        bundle.putString("file", file.getName());
        msg.setData(bundle);
        msg.sendToTarget();
    }

    // TODO Break out messages to seperate classes

    // File Pusher Protocol
    // The file pusher protocol is a simple order dependent protocol for copying files
    // over bluetooth. Files and directories are relative to a virtual root on the
    // recieving server.
    //
    // 1 byte - Message Type
    // variable - Dependent on message type
    //
    // File Message Type
    // 1 byte - Message Type (FILE = 0x01)
    // 2 byte - Filename Length
    // n byte - Filename
    // 2 byte - Container Name (Target Directory) Length
    // n byte - Container Name (Target Directory)
    // 1 byte - Compression: 0 = None
    // 4 byte - File length
    // n byte - File contents
    // 8 byte - CRC
    //
    // Directory Message Type
    // 1 byte - Message Type (DIRECTORY = 0x02)
    // 2 byte - Directory Name Length
    // n byte - Directory Name
    // 2 byte - Child Count
    //
    // Stop Message Type
    // 1 byte - Message Type (STOP = 0x03)
    //
    // Keep Alive Message Type
    // 1 byte - Message Type (KEEP_ALIVE = 0x04)
    //
    // File Ack Message Type
    // 1 byte - Message Type (FILE_ACK = 0x05)
    //

    /**
     * Write a file over the connected socket
     */
    private void writeFile(File file, File destinationDirectory) throws IOException {
        Log.d(TAG, "Sending File: " + file);

        // Aquire a permit before writing a file so that when we
        // attempt to aquire it again at the end of writing
        // to the stream it will block until the listening thread
        // when it gets a FileAck message releases the permit.
//            try {
        // mmWaitOnFileAckLock.acquire();
        waitOnFileAck.set(true);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

        // type
        outStream.writeByte(FilePushMessage.TYPE_FILE);

        outStream.writeUTF(file.getName());

        if (destinationDirectory != null) {
            outStream.writeUTF(destinationDirectory.getPath());
        } else {
            outStream.writeShort(0);
        }

        // compression
        outStream.writeByte(FilePushMessage.COMPRESSION_NONE);

        // file chunk
        outStream.writeInt((int) file.length());
        // copy the file to the stream
        CRC32 crc = new CRC32();
        FileInputStream inputStream = new FileInputStream(file);
        int totalWritten = 0;
        while (true) {
            int numBytes = inputStream.read(writeBuffer);
            if (numBytes == -1) {
                break;
            }
            totalWritten += numBytes;
            crc.update(writeBuffer, 0, numBytes);
            outStream.write(writeBuffer, 0, numBytes);
        }
        outStream.writeLong(crc.getValue());
        outStream.flush();

        Log.d(TAG, "Sent Bytes: " + totalWritten);
        Log.d(TAG, "Sent CRC: " + crc.getValue());

        // once the writes are complete to the stream we wait for an ack from the device
        try {
            Log.d(TAG, "Waiting on file ack");
            while (waitOnFileAck.get()) {
                Thread.sleep(1);
                //mmWaitOnFileAckLock.acquire();
            }
            Log.d(TAG, "File ack received");
        } catch (InterruptedException e) {
            Log.e(TAG, "Interrupted while waiting for FileAck", e);
        }
    }

    private void writeDirectory(File dir, File destinationContainer) throws IOException {
        Log.d(TAG, "Sending Directory: " + dir.getName() + " -> " + destinationContainer.getPath());
        File[] children = dir.listFiles();
        outStream.writeByte(FilePushMessage.TYPE_DIRECTORY);
        outStream.writeUTF(destinationContainer.getPath());
        outStream.writeShort(children.length);
        for (File subfile : children) {
            if (subfile.isFile()) {
                writeFile(subfile, destinationContainer);
            } else if (subfile.isDirectory()) {
                writeDirectory(subfile, new File(destinationContainer, subfile.getName()));
            }
        }
    }

    private void writeStop() throws IOException {
        Log.d(TAG, "Sending stop message");
        outStream.writeByte(FilePushMessage.TYPE_STOP);
        outStream.flush();
    }

    // Disconnect everything and perform an orderly shutdown
    private void disconnect() {
        if (!socket.isConnected()) {
            return;
        }
        Log.d(TAG, "Closing thread: " + socket.getRemoteDevice().getName());


        try {
            Log.d(TAG, "Writing stop message: " + socket.getRemoteDevice().getName());
            writeStop();
        } catch (IOException e) {
            Log.e(TAG, "Could not write stop message.", e);

        }

        try {
            Log.d(TAG, "Flushing outpustream: " + socket.getRemoteDevice().getName());
            outStream.flush();
        } catch (IOException e) {
            Log.e(TAG, "Could not flush the outpustream", e);
        }
        try {
            Log.d(TAG, "Closing Output: " + socket.getRemoteDevice().getName());
            outStream.close();
        } catch (IOException e) {
            Log.e(TAG, "Could not close the Output", e);
        }
        try {
            Log.d(TAG, "Closing Input: " + socket.getRemoteDevice().getName());
            inStream.close();
        } catch (IOException e) {
            Log.e(TAG, "Could not close the Input", e);
        }

        try {
            Log.d(TAG, "Closing socket: " + socket.getRemoteDevice().getName());
            socket.close();
        } catch (IOException e) {
            Log.e(TAG, "Could not close the connection to socket", e);
        }
    }
}