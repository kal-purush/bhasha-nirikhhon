package website.lucaswork.clipboardOcr;

import com.baidu.aip.ocr.AipOcr;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
@Configuration
public class OcrApplication {

	@Value("${baidu.api.id}")
	private String apiId;

	@Value("${baidu.api.key}")
	private String apiKey;

	@Value("${baidu.api.secret.key}")
	private String secretKey;

	public static void main(String[] args) {
		SpringApplication.run(OcrApplication.class, args);
	}

	@Bean
	public AipOcr getAipOcr() {
		return new AipOcr(apiId, apiKey, secretKey);
	}
}