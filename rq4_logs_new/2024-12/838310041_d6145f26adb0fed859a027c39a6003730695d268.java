package applesquare.moment.post.controller;

import applesquare.moment.common.dto.PageRequestDTO;
import applesquare.moment.common.dto.PageResponseDTO;
import applesquare.moment.common.exception.ResponseMap;
import applesquare.moment.post.dto.MomentDetailReadAllResponseDTO;
import applesquare.moment.post.service.MomentSearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/posts/moments")
public class MomentSearchController {
    private final MomentSearchService momentSearchService;


    @GetMapping("/search")
    public ResponseEntity<Map<String, Object>> searchDetail(@RequestParam(value = "type", required = false, defaultValue = "DETAIL") PostReadType type,
                                             @RequestParam(value = "size", required = false, defaultValue = "10") int size,
                                             @RequestParam(value = "cursor", required = false) String cursor,
                                             @RequestParam(value = "keyword", required = false) String keyword){
        // 페이지 요청 설정
        PageRequestDTO pageRequestDTO=PageRequestDTO.builder()
                .size(size)
                .cursor(cursor)
                .keyword(keyword)
                .build();

        // 응답 객체 구성
        ResponseMap responseMap=new ResponseMap();

        switch(type){
            case DETAIL:
                // 모먼트 목록 조회
                PageResponseDTO<MomentDetailReadAllResponseDTO> pageResponseDTO=momentSearchService.searchDetail(pageRequestDTO);
                responseMap.put("content", pageResponseDTO.getContent());
                responseMap.put("hasNext", pageResponseDTO.isHasNext());
                break;
            case THUMBNAIL:
                throw new IllegalArgumentException("지원하지 않는 조회 타입입니다. (type="+type+")");
        }

        responseMap.put("message", "모먼트 검색에 성공했습니다.");

        return ResponseEntity.status(HttpStatus.OK).body(responseMap.getMap());
    }
}