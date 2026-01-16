package com.example.tagletagle.meta;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
@RestController
@RequestMapping("/api/metadata")
public class MetadataController {

	private final MetadataService metadataService;

	public MetadataController(MetadataService metadataService) {
		this.metadataService = metadataService;
	}

	// 1️⃣ 단일 URL 메타데이터 가져오기
	@GetMapping()
	public ResponseEntity<Map<String, String>> getMetadata(@RequestParam("url") String url) {
		Map<String, String> metadata = metadataService.fetchMetadata(url);
		return ResponseEntity.ok(metadata);
	}
/*	@PostMapping
	public ResponseEntity<Map<String, String>> getMetadata(@RequestBody Map<String, String> requestBody) {
		String url = requestBody.get("url"); // JSON에서 "url" 값을 추출
		if (url == null || url.isEmpty()) {
			return ResponseEntity.badRequest().body(Map.of("error", "URL is required"));
		}

		Map<String, String> metadata = metadataService.fetchMetadata(url);
		return ResponseEntity.ok(metadata);
	}*/

	// 2️⃣ 여러 개의 URL 메타데이터 가져오기 (멀티스레딩 활용)
	/*@PostMapping("/batch")
	public ResponseEntity<List<Map<String, String>>> getMultipleMetadata(@RequestBody List<String> urls) {
		List<Map<String, String>> metadataList = metadataService.fetchMultipleMetadata(urls);
		return ResponseEntity.ok(metadataList);
	}*/
}