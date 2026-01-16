package kr.co.teacherforboss.converter;


import kr.co.teacherforboss.domain.Exam;
import kr.co.teacherforboss.domain.Member;
import kr.co.teacherforboss.domain.MemberAnswer;
import kr.co.teacherforboss.domain.MemberExam;
import kr.co.teacherforboss.domain.Question;
import kr.co.teacherforboss.domain.QuestionChoice;
import kr.co.teacherforboss.web.dto.ExamResponseDTO;

import java.time.LocalDateTime;

public class ExamConverter {

    public static ExamResponseDTO.TakeExamsDTO toTakeExamsDTO(MemberExam memberExam) {
        return ExamResponseDTO.TakeExamsDTO.builder()
                .memberExamId(memberExam.getId())
                .createdAt(LocalDateTime.now())
                .build();
    }

    public static MemberAnswer toMemberAnswerList(Question question, QuestionChoice questionChoice) {
        return MemberAnswer.builder()
                .question(question)
                .questionChoice(questionChoice)
                .build();
    }

    public static MemberExam toMemberExam(Member member, Exam exam) {
        return MemberExam.builder()
                .member(member)
                .exam(exam)
                .build();
    }
}