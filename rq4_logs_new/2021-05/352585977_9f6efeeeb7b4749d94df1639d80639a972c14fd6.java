package ua.edu.sumdu.j2ee.kiptenko.demo.controller;

import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import ua.edu.sumdu.j2ee.kiptenko.demo.converter.DocxGenerator;
import ua.edu.sumdu.j2ee.kiptenko.demo.converter.JsonConverter;
import ua.edu.sumdu.j2ee.kiptenko.demo.model.NewsPojo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;

public class DocumentController {
    public static ResponseEntity<InputStreamResource> getDocument(String baseURLsources, String apiKey, String keyWord) throws IOException, URISyntaxException {
        NewsPojo nptest = JsonConverter.createObject(JsonConverter.getStringJson(baseURLsources, apiKey, keyWord));
        byte[] doc = DocxGenerator.generateTemplate(nptest);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentDispositionFormData("attachment", "News.docx");
        headers.setContentType(new MediaType("application", "vnd.openxmlformats-officedocument.wordprocessingml.document"));
        headers.setContentLength(doc.length);
        InputStreamResource inputStreamResource = new InputStreamResource(new ByteArrayInputStream(doc));
        return ResponseEntity.ok().headers(headers).body(inputStreamResource);
    }
}