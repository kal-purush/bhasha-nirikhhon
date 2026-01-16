package common;

import jdk.jshell.spi.ExecutionControl;

public class JobEncoderDecoder {
    /**
     * @param jobId        - number of job
     * @param contentImage - Content image, bytes
     * @param styleImage   - Style image, bytes
     * @return Json serialized with same fields as input, but in BASE64(except for jobId)
     */
    public static String encode(int jobId, byte[] contentImage, byte[] styleImage) {

        return null; // jsonString
    }


    /**
     * @param jsonString - Json serialized with fields: jobId, contentImage, styleImage
     * @return Object with fields int jobId, byte[] resultImage
     */
    public static String decode(String jsonString) {
        return null;  // result - объект с полями int jobId, byte[] resultImage
    }
}