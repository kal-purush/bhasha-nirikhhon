import android.content.Context;
import org.tensorflow.lite.Interpreter;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TFLiteInference {
    private Interpreter tfliteInterpreter;
    private final int OUTPUT_SIZE; // Adjust outputSize based on your model

    public TFLiteInference(Context context) {
        OUTPUT_SIZE = 10; // Assuming 10 classes
        try {
            // Load the TensorFlow Lite model from the assets folder
            MappedByteBuffer tfliteModel = loadModelFile(context, "model.tflite");
            Interpreter.Options options = new Interpreter.Options();
            tfliteInterpreter = new Interpreter(tfliteModel, options);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Helper method to load the TensorFlow Lite model from the assets folder
    private MappedByteBuffer loadModelFile(Context context, String modelFilename) throws IOException {
        FileInputStream fileInputStream = new FileInputStream("assets/" + modelFilename);
        FileChannel fileChannel = fileInputStream.getChannel();
        long startOffset = fileChannel.size();
        return fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, startOffset);
    }

    // Perform inference with input data
    public float[] predict(float[] inputData) {
        float[][] output = new float[1][OUTPUT_SIZE]; // Adjust outputSize based on your model
        tfliteInterpreter.run(inputData, output);
        return output[0];
    }
}