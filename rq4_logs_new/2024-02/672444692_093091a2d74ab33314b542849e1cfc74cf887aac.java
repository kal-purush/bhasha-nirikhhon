package kr.ac.kumoh.illdang100.tovalley.util;

import kr.ac.kumoh.illdang100.tovalley.domain.FileRootPathVO;
import kr.ac.kumoh.illdang100.tovalley.domain.ImageFile;
import kr.ac.kumoh.illdang100.tovalley.service.S3Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ImageUtil {

    public static List<ImageFile> uploadImageFile(S3Service s3Service, List<MultipartFile> multipartFiles) throws IOException {
        List<ImageFile> uploadImageFiles = new ArrayList<>();
        if (ListUtil.isEmptyList(multipartFiles)) {
            uploadImageFiles = s3Service.upload(multipartFiles, FileRootPathVO.LOST_FOUND_BOARD_PATH);
        }
        return uploadImageFiles;
    }
}