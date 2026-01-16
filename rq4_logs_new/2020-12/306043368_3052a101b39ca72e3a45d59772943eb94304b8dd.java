package org.example.module.common.file.service.impl;

import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.Region;
import com.qiniu.storage.UploadManager;
import com.qiniu.util.Auth;
import org.example.common.enums.ResultCode;
import org.example.common.exception.BusinessException;
import org.example.common.util.FileUtil;
import org.example.config.oss.QiniuConfig;
import org.example.module.common.file.domain.dto.FileInfoDTO;
import org.example.module.common.file.service.IFileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;

/**
 * @author linzhaoming
 * @since 2020/12/15
 **/
@Service("qiniuFileService")
public class QiniuFileServiceImpl implements IFileService {

    private final static Logger log = LoggerFactory.getLogger(QiniuFileServiceImpl.class);

    @Resource
    private QiniuConfig qiniuConfig;

    @Override
    public FileInfoDTO upload(MultipartFile file) {
        FileInfoDTO dto = new FileInfoDTO();

        dto.setFileName(file.getOriginalFilename());
        dto.setFileSize(file.getSize());

        try {
            Configuration cfg = new Configuration(Region.region0());
            UploadManager uploadManager = new UploadManager(cfg);
            String key = FileUtil.buildUniqueFileName(file.getOriginalFilename());
            Auth auth = Auth.create(qiniuConfig.getAccessKey(), qiniuConfig.getSecretKey());
            String token = auth.uploadToken(qiniuConfig.getBucketName());
            Response response = uploadManager.put(file.getInputStream(), key, token, null, null);
            String fileUrl = qiniuConfig.getPrefixLink() + key;
            dto.setFileUrl(fileUrl);
        } catch (Exception e) {
            log.error("Func[upload] occur exception", e);
            throw new BusinessException(ResultCode.FAILURE);
        }

        return dto;
    }
}