package org.detective.controller.Inquery;

import org.detective.controller.Member.MemberController;
import org.detective.dto.InqueryDTO;
import org.detective.entity.Inquery;
import org.detective.entity.User;
import org.detective.services.inquery.InqueryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/inquery")
public class InqueryController {

    private final InqueryService inqueryService;
    private final MemberController memberController;

    public InqueryController(InqueryService inqueryService, MemberController memberController) {
        this.inqueryService = inqueryService;
        this.memberController = memberController;
    }

    @PostMapping("/insert")
    public ResponseEntity<?> saveInquery(@RequestBody InqueryDTO inqueryDTO) {
        try {
            System.out.println("inqueryDTO : " + inqueryDTO);

            User user = memberController.getUserInfo();

            Inquery inquery = new Inquery(
                    null,  // id는 자동 생성되므로 null로 설정
                    user,
                    inqueryDTO.getTitle(),
                    inqueryDTO.getEmail(),
                    inqueryDTO.getCategory(),
                    inqueryDTO.getContent(),
                    Inquery.ResponseStatus.PENDING,
                    null
            );
            Inquery result = inqueryService.saveInquery(inquery);

            return ResponseEntity.ok("문의가 성공적으로 접수됨");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("문의 접수 에러 발생함 " + e.getMessage());
        }
    }



}