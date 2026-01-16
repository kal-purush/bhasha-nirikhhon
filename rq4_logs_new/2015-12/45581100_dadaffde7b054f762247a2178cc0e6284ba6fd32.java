package incircle.api;

import incircle.config.WebConfig;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by Julian on 12/23/2015.
 */
@RestController
public class ResourceController {
    @Autowired
    private WebConfig webConfig;

    @RequestMapping(value="/images/{username}", method= RequestMethod.GET, produces = MediaType.IMAGE_JPEG_VALUE)
    public byte[] getImages(@PathVariable String username) throws IOException {
        File imageFile = new File(webConfig.resourcePaths.imageFolderPath() + username + ".jpg");
        byte[] byteArray = IOUtils.toByteArray(new FileInputStream(imageFile));
        return byteArray;
    }

    @RequestMapping(value="/videos/{username}", method= RequestMethod.GET, produces = "video/mp4")
    public ResponseEntity<byte[]> getVideos(@PathVariable String username) throws IOException {
        File videoFile = new File(webConfig.resourcePaths.videoFolderPath() + username + ".mp4");
        byte[] byteArray = IOUtils.toByteArray(new FileInputStream(videoFile));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentLength(videoFile.length());
        return new ResponseEntity<byte[]>(byteArray, headers, HttpStatus.OK);
    }
}