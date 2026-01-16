package com.lotdiz.adminservice.controller.restcontroller;

import com.lotdiz.adminservice.dto.response.GetMakerResponseDto;
import com.lotdiz.adminservice.dto.response.GetMakerSearchResponseDto;
import com.lotdiz.adminservice.dto.response.GetProjectResponseDto;
import com.lotdiz.adminservice.service.ProjectInfoService;
import com.lotdiz.adminservice.utils.SuccessResponse;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class ProjectRestController {

  private final ProjectInfoService projectInfoService;

  @GetMapping("/projects")
  public ResponseEntity<SuccessResponse<Map<String, List<GetProjectResponseDto>>>> getProjects(
      @PageableDefault(
              page = 0,
              size = 20,
              sort = {"createdAt"},
              direction = Sort.Direction.DESC)
          Pageable pageable) {
    List<GetProjectResponseDto> getProjectResponseDtos = projectInfoService.getProjects(pageable);
    return ResponseEntity.ok()
        .body(
            SuccessResponse.<Map<String, List<GetProjectResponseDto>>>builder()
                .code(String.valueOf(HttpStatus.OK.value()))
                .message(HttpStatus.OK.name())
                .detail("프로젝트 조회 성공")
                .data(Map.of("projects", getProjectResponseDtos))
                .build());
  }

//  @PostMapping("/projects/{projectId}/auth")
//  public ResponseEntity<SuccessResponse<Object>> authorizeProject(
//      @PathVariable("projectId") Long projectId) {
//    return ResponseEntity.ok().body(null);
//  }
//
//  @GetMapping("/makers")
//  public ResponseEntity<SuccessResponse<Map<String, List<GetMakerResponseDto>>>> getMakers(
//      @RequestParam("page") int page,
//      @RequestParam("size") int size,
//      @RequestParam("sort") String sort) {
//    return ResponseEntity.ok().body(null);
//  }
//
//  @GetMapping("/makers/search")
//  public ResponseEntity<SuccessResponse<Map<String, List<GetMakerSearchResponseDto>>>> getMakers(
//      @RequestParam("query") String query,
//      @RequestParam("page") int page,
//      @RequestParam("size") int size,
//      @RequestParam("sort") String sort) {
//    return ResponseEntity.ok().body(null);
//  }
}