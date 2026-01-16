package comunication;
   
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 *Class integrated to Connection.java that is responsable for deal with the stream
 * of the socket
 * @author Juliana
 */
public class ConnectionIO {
    private final InputStream input; 
    private final OutputStream output;
    
    /**
     *  Turn the socket to an object of input and other for output
     * @param socket
     * @throws IOException
     */
    public ConnectionIO(Socket socket) throws IOException{
        input = socket.getInputStream();
        output = socket.getOutputStream();
    }

    /**
     *That method starts with a empty byte array "byteArrayInputed",
     * call the methods processOutput, passing the bytesOutput by param, and processInput, the last one 
     * responsible for set a new value for "byteArrayInputed"   
     * @param bytesOutput
     * @return the value of the inputStream as the value of byteArrayInputed
     */
    public byte[] processStream(byte[] bytesOutput){
        byte[] byteArrayInputed = {};
        try {
            processOutput(output, bytesOutput);
            byteArrayInputed = processInput(input);
        } catch (IOException ex) {
            //tratar isso aqui depois
        }
        return byteArrayInputed;
    }
    
    private byte[] processInput(InputStream input) throws IOException{
        DataInputStream dataInputStream = new DataInputStream(input);
        byte buffer[] = new byte[dataInputStream.available()]; 
        dataInputStream.readFully(buffer);
        return buffer;
    }
    

    private void processOutput(OutputStream output, byte[] bytesOutput){
        if(bytesOutput.length > 0){
            try {
                output.write(bytesOutput, 0, bytesOutput.length);
                output.flush(); 
            } catch (IOException ex) {
                //Tratar isso aqui depois
            }         
        } 
    }    
    
}