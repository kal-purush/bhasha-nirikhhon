package com.metamong.mt.global.file;

import java.io.IOException;
import java.io.InputStream;

/**
 * <p>파일을 업로드하는 객체.
 * <p>테스트 환경에서는 로컬 파일시스템에 파일을 저장하는 파일 업로더가 반환되고, 배포 환경에서는
 * AWS S3 등 클라우드 오브젝트 스토리지에 파일을 저장하는 파일 업로더가 반환된다.
 */
public interface FileUploader {

    /**
     * 클라이언트에서 파일을 업로드할 URL을 생성한다. 클라이언트에서는 이 메소드를 통해 생성된 URL로 파일을 업로드해야 한다.
     * 배포 환경에서는 presigned URL이 반환된다.
     * 
     * @param filename URL을 생성할 파일명
     * @return 파일 업로드 URL
     */
    String generateUploadUrl(String filename);
    
    /**
     * 로컬에 파일을 저장하는 메소드. 배포 환경에서는 클라이언트에서 직접 오브젝트 스토리지로 파일을 업로드하기 때문에 해당 메소드를 지원하지 않는다.
     * 
     * @param filename 저장할 파일의 이름
     * @param is 파일 데이터가 담긴 InputStream
     * @throws UnsupportedOperationException 배포 환경에서 해당 메소드를 호출할 경우
     * @throws IOException 파일 저장 시 문제가 발생할 경우
     */
    void storeFile(String filename, InputStream is)
            throws UnsupportedOperationException, IOException;
    
    /**
     * 로컬에 파일을 저장하는 메소드. 배포 환경에서는 클라이언트에서 직접 오브젝트 스토리지로 파일을 업로드하기 때문에 해당 메소드를 지원하지 않는다.
     * 
     * @param filename 저장할 파일의 이름
     * @param data 파일 데이터가 담긴 byte array
     * @throws UnsupportedOperationException 배포 환경에서 해당 메소드를 호출할 경우
     * @throws IOException 파일 저장 시 문제가 발생할 경우
     */
    void storeFile(String filename, byte[] data)
            throws UnsupportedOperationException, IOException;
}