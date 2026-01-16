package com.example.demo.repository;

import com.example.demo.Landmark.entity.Landmark;
import com.example.demo.Landmark.entity.LandmarkComment;
import com.example.demo.Landmark.repository.LandmarkCommentRepository;
import com.example.demo.Landmark.repository.LandmarkRepository;
import com.example.demo.member.dto.MemberDTO;
import com.example.demo.member.entity.Member;
import com.example.demo.member.repository.MemberRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalTime;
import java.util.List;
import java.util.Optional;

@SpringBootTest
public class LandmarkCommentRepositoryTest {

    @Autowired
    LandmarkCommentRepository repository;
    @Autowired
    MemberRepository memberRepository;
    @Autowired
    LandmarkRepository landmarkRepository;
//    예제와 달리 리포지토리.save 안할시 일단 한건도 등록 안됨

    @Test
    public void 댓글등록(){
//        멤버(사용자)생성
        Member member = Member.builder()
                .userId("green")
                .userPw("1234")
                .userName("박춘봉")
                .userRole("ROLE_USER")
                .userEmail("kkk@naver.com")
                .build();
        memberRepository.save(member);

//        랜드마크게시글 생성
        Landmark landmark = Landmark.builder()
                .landmarkNo(0)
                .writer(member).landmarkDay(2)
                .landmarkTime(LocalTime.of(12,30))
                .landmarkAddress("인천")
                .landmarkName("월미도")
                .landmarkOrigin(true)
                .landmarkShortDesc("월미도바이킹")
                .landmarkDesc("회먹고구경하고")
                .longitude("543")
                .latitude("1232")
                .build();
        landmarkRepository.save(landmark);

//        **댓글생성**
        LandmarkComment comment = LandmarkComment.builder()
                .landmarkNo(landmark)
                .writer(member)
                .landmarkCommentContent("댓글이에요222")
                .build();
        repository.save(comment);   //  db저장해
    }

    @Test
    public void 댓글목록조회() {
        List<LandmarkComment> list = repository.findAll();
        for(LandmarkComment comment : list) {
            System.out.println(comment);
        }
    }

    @Test
    public void 댓글단건조회() {
        Optional<LandmarkComment> result = repository.findById(16);
//        ?결과없음?
        if(result.isPresent()) {
            LandmarkComment comment = result.get();
            System.out.println(comment);
        }
    }

    @Test
    public void 댓글수정() {
        // 기존 댓글 조회
        Optional<LandmarkComment> result = repository.findById(0);
        LandmarkComment comment = result.get();
        // 일부 내용 변경
        comment.setLandmarkCommentContent("댓글내용수정수정수정");
        // 댓글 수정
        repository.save(comment);
    }
    @Test
    public void 댓글삭제() {
        repository.deleteById(0);
    }

}