package com.petid.api.content;
import lombok.RequiredArgsConstructor;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.petid.api.common.RequestUtil;
import com.petid.domain.content.ContentService;
import com.petid.domain.content.model.Content;
import com.petid.domain.type.Category;

import jakarta.servlet.http.HttpServletRequest;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/v1/content")
@RequiredArgsConstructor
public class ContentController {

	private final ContentService contentService;

    // Create a new content
    @PostMapping
    public ResponseEntity<Content> createContent(HttpServletRequest request, @RequestBody Content content) {
    	long memberId = RequestUtil.getMemberIdFromRequest(request);
        Content createdContent = contentService.createContent(content, memberId);
        return new ResponseEntity<>(createdContent, HttpStatus.CREATED);
    }

    // Get all contents
    @GetMapping
    public ResponseEntity<List<Content>> getContentsByCategory(HttpServletRequest request, @RequestParam Category category) {
    	long memberId = RequestUtil.getMemberIdFromRequest(request);
        List<Content> contents = contentService.getContentsByCategory(category, memberId);
        return new ResponseEntity<>(contents, HttpStatus.OK);
    }

    // Get content by ID
    @GetMapping("/{id}")
    public ResponseEntity<Content> getContentById(@PathVariable Long id) {
        Optional<Content> content = contentService.getContentById(id);
        return content.map(ResponseEntity::ok)
                      .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    // Update content by ID
    @PutMapping("/{id}")
    public ResponseEntity<Content> updateContent(@PathVariable Long id, @RequestBody Content content) {
        Optional<Content> updatedContent = contentService.updateContent(id, content);
        return updatedContent.map(ResponseEntity::ok)
                             .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    // Delete content by ID
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteContent(@PathVariable Long id) {
        boolean isDeleted = contentService.deleteContent(id);
        if (isDeleted) {
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        }
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

}