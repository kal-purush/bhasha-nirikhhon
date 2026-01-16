package meteocontroller.photo.uploader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import meteocontroller.MeteoLogger;
import meteocontroller.dto.PhotoDto;
import org.apache.commons.net.ftp.FTPClient;

/**
 *
 * @author Jan Novak <novano@mail.muni.cz>
 */
public class PhotoUploaderWorker implements Runnable {

    private PhotoDto photo;
    private FTPClient client;

    public PhotoUploaderWorker(PhotoDto photo) {
        if (photo == null) {
            throw new IllegalArgumentException("Photo is null!");
        }
        this.photo = photo;
    }

    @Override
    public void run() {
        client = PhotoUploaderImpl.getConnection();
        if (client == null) {
            throw new NullPointerException("FTP client is null!");
        }
        uploadPhotosToServer();
    }

    private void uploadPhotosToServer() {
        MeteoLogger.log("upload timer activated");

        try {
            String remoteFile = "radekMeteo/actual.jpg";
            try (InputStream inputStream = new FileInputStream(photo.getFile())) {
                MeteoLogger.log("storing to FTP server");
                client.storeFile(remoteFile, inputStream);
                MeteoLogger.log("storing to FTP server completed");
                MeteoLogger.log("Uploading file '" + photo.getFile().getName() + "' as remote file '" + remoteFile + "'.");
            }
        } catch (SocketException ex) {
            PhotoUploaderImpl.closeConnection(client);
            client = PhotoUploaderImpl.getConnection();
            //TODO pockej treba 10s a pak az se znova pripoj .. sak vis 
            uploadPhotosToServer();
        } catch (IOException ex) {
            MeteoLogger.log(ex);
        } finally {
            if (client != null) {
                PhotoUploaderImpl.closeConnection(client);
            }
        }

    }

}