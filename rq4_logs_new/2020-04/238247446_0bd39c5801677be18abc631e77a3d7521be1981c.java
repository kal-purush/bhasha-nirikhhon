package by.bsuir.tattoo4u.service.impl;

import by.bsuir.tattoo4u.entity.Photo;
import by.bsuir.tattoo4u.entity.PhotoUpload;
import by.bsuir.tattoo4u.repository.PhotoRepository;
import by.bsuir.tattoo4u.service.PhotoService;
import by.bsuir.tattoo4u.service.ServiceException;
import com.cloudinary.Cloudinary;
import com.cloudinary.utils.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

@Service
public class PhotoServiceImpl implements PhotoService {

    @Value("${cloudinary.cloudName}")
    private String cloudName;

    @Value("${cloudinary.apiSecret}")
    private String apiSecret;

    @Value("${cloudinary.apiKey}")
    private String apiKey;

    private PhotoRepository photoRepository;
    private Cloudinary cloudinary = new Cloudinary();

    @Autowired
    public PhotoServiceImpl(PhotoRepository photoRepository) {
        this.photoRepository = photoRepository;
    }

    @Override
    public Photo save(PhotoUpload photoUpload) throws ServiceException {
        cloudinary.config.cloudName = cloudName;
        cloudinary.config.apiSecret = apiSecret;
        cloudinary.config.apiKey = apiKey;

        try {
            Map uploadResult = cloudinary.uploader().upload(
                    photoUpload.getFile().getBytes(),
                    ObjectUtils.asMap("resource_type", "image")
            );

            photoUpload.setPublicId((String) uploadResult.get("public_id"));

            Object version = uploadResult.get("version");
            if (version instanceof Integer) {
                photoUpload.setVersion(new Long((Integer) version));
            } else {
                photoUpload.setVersion((Long) version);
            }

            photoUpload.setSignature((String) uploadResult.get("signature"));
            photoUpload.setFormat((String) uploadResult.get("format"));
            photoUpload.setResourceType((String) uploadResult.get("resource_type"));

            Photo photo = new Photo();
            photo.setUrl(photoUpload.getUrl(cloudinary));

            photoRepository.save(photo);

            return photo;
        } catch (IOException | RuntimeException e) {
            throw new ServiceException(e);
        }
    }
}