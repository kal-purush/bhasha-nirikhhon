package org.detective.config;
import com.siot.IamportRestClient.IamportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration  // 이 클래스가 스프링 설정 클래스를 나타냄
public class AppConfig {
    // 아임포트 API 사용을 위한 API 키와 시크릿 키
    String apiKey = "5644472518788028";  // 아임포트에서 발급된 API 키
    String secretKey = "4LUok7Vwn7o62YjKSWLiEP3GJPluc1TCMGSkGNdCuepSntL6GjPuzOSilfFLD5QmzzlPAQW4CsK1VAma";  // 아임포트에서 발급된 시크릿 키

    @Bean  // 스프링 컨텍스트에서 관리되는 IamportClient 빈을 생성
    public IamportClient iamportClient() {
        return new IamportClient(apiKey, secretKey);  // API 키와 시크릿 키를 사용하여 IamportClient 인스턴스를 생성 및 반환
    }
}//