package io.github.xpakx.images.upload;

import io.github.xpakx.images.common.types.Result;
import io.github.xpakx.images.image.error.CouldNotStoreException;
import io.github.xpakx.images.image.error.EmptyFilenameException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class UploadService {
    Path root = Path.of("uploads");
    Path avatarRoot = root.resolve("avatars");

    public Result<String> trySave(MultipartFile file) {
        return trySave(file, root);
    }

    public Result<String> trySaveAvatar(MultipartFile file) {
        return trySave(file, avatarRoot);
    }

    private Result<String> trySave(MultipartFile file, Path root) {
        // TODO: name with hash/timestamp?
        if(Objects.isNull(file.getOriginalFilename()) || file.getOriginalFilename().isEmpty()) {
            return new Result.Err<>(new EmptyFilenameException("Filename cannot be empty!"));
        }

        // TODO: better file structure and check mimetype

        String name = file.getOriginalFilename();
        try {
            if (!Files.exists(root)) {
                Files.createDirectories(root);
            }
            Files.deleteIfExists(root.resolve(name));
            Files.copy(file.getInputStream(), root.resolve(name));
        } catch (Exception e) {
            return new Result.Err<>(new CouldNotStoreException("Could not store the file"));
        }
        return new Result.Ok<>(name);
    }
}