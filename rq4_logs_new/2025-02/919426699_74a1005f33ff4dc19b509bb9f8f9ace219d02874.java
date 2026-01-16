package com.example.tagletagle.meta;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class JsoupMetadataService {

	public Map<String, String> fetchMetadata(String url) {
		Map<String, String> metadata = new HashMap<>();
		try {
			// URL에서 HTML 문서 가져오기
			Document document = Jsoup.connect(url)
				.userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")
				.timeout(10000)
				.get();

			// 네이버 블로그 예외 처리
			if (url.contains("blog.naver.com")) {
				metadata = fetchNaverBlogMetadata(document, url);
			}
			// 티스토리 예외 처리
			else if (url.contains("tistory.com")) {
				metadata = fetchTistoryMetadata(document);
			}
			// 기본 Open Graph 처리
			else {
				metadata = fetchOpenGraphMetadata(document);
			}
		} catch (IOException e) {
			metadata.put("error", "Failed to fetch metadata: " + e.getMessage());
		}

		return metadata;
	}

	// 네이버 블로그 메타데이터 처리
	private Map<String, String> fetchNaverBlogMetadata(Document document, String url) {
		Map<String, String> metadata = new HashMap<>();
		try {
			// 네이버 블로그는 본문이 iframe 내부에 존재
			Element iframe = document.selectFirst("iframe");
			if (iframe != null) {
				String iframeSrc = iframe.attr("src");

				// 상대 경로이면 절대 경로로 변환
				if (!iframeSrc.startsWith("http")) {
					iframeSrc = "https://blog.naver.com" + iframeSrc;
				}

				// iframe 내부 HTML을 다시 Jsoup으로 가져옴
				document = Jsoup.connect(iframeSrc)
					.userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")
					.timeout(10000)
					.get();
			}

			// 메타데이터 추출
			metadata.put("title", getMetaTag(document, "og:title"));
			metadata.put("image", getMetaTag(document, "og:image"));
			metadata.put("author", getMetaTag(document, "article:author"));

			// 만약 author 태그가 없으면 기본 author 정보 검색
			if (metadata.get("author") == null) {
				metadata.put("author", getMetaTag(document, "og:author"));
			}
		} catch (Exception e) {
			metadata.put("error", "Failed to fetch metadata from Naver blog: " + e.getMessage());
		}
		return metadata;
	}


	// 티스토리 메타데이터 처리
	private Map<String, String> fetchTistoryMetadata(Document document) {
		Map<String, String> metadata = new HashMap<>();
		try {
			metadata.put("title", getMetaTag(document, "og:title"));
			metadata.put("image", getMetaTag(document, "og:image"));
			metadata.put("author", getMetaTag(document, "article:author"));

			if (metadata.get("author") == null) {
				metadata.put("author", getMetaTag(document, "og:author"));
			}
		} catch (Exception e) {
			metadata.put("error", "Failed to fetch metadata from Tistory: " + e.getMessage());
		}
		return metadata;
	}

	// Open Graph 태그 처리 (기본 메타데이터 처리)
	private Map<String, String> fetchOpenGraphMetadata(Document document) {
		Map<String, String> metadata = new HashMap<>();
		try {
			metadata.put("title", getMetaTag(document, "og:title"));
			metadata.put("image", getMetaTag(document, "og:image"));
			metadata.put("author", getMetaTag(document, "article:author"));

			if (metadata.get("author") == null) {
				metadata.put("author", getMetaTag(document, "og:author"));
			}
			if (metadata.get("author") == null) {
				metadata.put("author", getMetaTag(document, "name=author"));
			}

			// 제목이 없을 경우 기본 <title> 태그 사용
			if (metadata.get("title") == null || metadata.get("title").isEmpty()) {
				metadata.put("title", document.title());
			}
		} catch (Exception e) {
			metadata.put("error", "Failed to fetch Open Graph metadata: " + e.getMessage());
		}
		return metadata;
	}

	// 메타 태그 읽기 유틸 메서드
	private String getMetaTag(Document document, String property) {
		Element metaTag = document.selectFirst("meta[property='" + property + "']");
		return metaTag != null ? metaTag.attr("content") : null;
	}
}