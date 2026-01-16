package top.ticho.boot.minio;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import top.ticho.boot.minio.component.MinioTemplate;
import top.ticho.boot.minio.prop.MinioProperty;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Objects;

/**
 * Minio文件下载,https://docs.minio.io/cn/java-client-api-reference.html#removeObject
 *
 * @author zhajianjun
 * @date 2022-07-13 22:40:25
 */
@Data
@Slf4j
public class MinioTemplateTest {
    private MinioTemplate minioTemplate;

    public MinioTemplateTest(MinioTemplate minioTemplate) {
        this.minioTemplate = minioTemplate;
    }

    public void fileSpliceChunk(String filePath) throws IOException {
        File sourceFile = new File(filePath);
        if (!sourceFile.exists()) {
            log.info(filePath + "文件不存在");
        }
        String chunkFolderPath = getChunkFolderPath(sourceFile);
        File chunkFolder = new File(chunkFolderPath);
        if (!chunkFolder.exists()) {
            chunkFolder.mkdirs();
        }
        // 分块大小 1MB
        long chunkSize = 1024 * 1024;
        // 分块数量
        long chunkNum = (long) Math.ceil(sourceFile.length() * 1.0 / chunkSize);
        log.info("分块总数：" + chunkNum);
        // 缓冲区大小
        byte[] bytes = new byte[1024];
        // 使用RandomAccessFile访问文件
        RandomAccessFile raf_read = new RandomAccessFile(sourceFile, "r");
        // 分块
        for (int i = 0; i < chunkNum; i++) {
            // 创建分块文件
            File chunkFile = new File(chunkFolderPath + File.separator + i);
            if (chunkFile.exists()) {
                chunkFile.delete();
            }
            boolean newFile = chunkFile.createNewFile();
            if (newFile) {
                // 向分块文件中写数据
                RandomAccessFile raf_write = new RandomAccessFile(chunkFile, "rw");
                int len;
                while ((len = raf_read.read(bytes)) != -1) {
                    raf_write.write(bytes, 0, len);
                    if (chunkFile.length() >= chunkSize) {
                        break;
                    }
                }
                raf_write.close();
                log.info("完成分块" + i);
            }
        }
        raf_read.close();
    }

    private String getChunkFolderPath(File sourceFile) {
        return sourceFile.getParentFile().getAbsolutePath() + File.separator + "chunk";
    }

    // 将分块文件上传至minio
    public void uploadChunk(String filePath, String bucket) {
        File sourceFile = new File(filePath);
        if (!sourceFile.exists()) {
            log.info(filePath + "文件不存在");
        }
        String chunkFolderPath = getChunkFolderPath(sourceFile);
        File chunkFolder = new File(chunkFolderPath);
        // 分块文件
        File[] files = chunkFolder.listFiles();
        if (Objects.isNull(files)) {
            return;
        }
        // 将分块文件上传至minio
        for (int i = 0; i < files.length; i++) {
            minioTemplate.uploadObject(bucket, files[i].getAbsolutePath(), "chunk/" + i, "application/octet-stream", null);
            log.info("上传分块成功" + i);
        }
    }


    public static void main(String[] args) throws IOException {
        String path = "";
        String chunkBucket = "";
        MinioProperty minioProperty = new MinioProperty();
        MinioTemplate minioTemplate = new MinioTemplate(minioProperty);
        MinioTemplateTest minioTemplateTest = new MinioTemplateTest(minioTemplate);
        minioTemplateTest.fileSpliceChunk(path);
        minioTemplateTest.uploadChunk(path, chunkBucket);
    }

}