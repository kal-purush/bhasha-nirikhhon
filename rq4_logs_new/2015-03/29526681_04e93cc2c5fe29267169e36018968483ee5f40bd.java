package uk.ac.cam.cl.echo.extrusionfinder.client;

import uk.ac.cam.cl.echo.extrusionfinder.client.servercopies.imagedata.ImageData;

/**
 * Created by oscar on 01/03/15.
 * Subclass of ImageData which represents a NV21 image
 */
public class NV21ImageData extends ImageData<byte[]> {
    public NV21ImageData(byte[] data, int width, int height) {
        super(data, width, height);
    }
}