package com.jia.study_tracker.service;

import com.jia.study_tracker.domain.Summary;
import com.jia.study_tracker.repository.SummaryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Summary 엔티티를 저장하는 책임을 가진 서비스 클래스
 * 트랜잭션 처리를 위해 SummaryGenerationService로부터 분리됨
 */
@Service
@RequiredArgsConstructor
public class SummarySaver {

    private final SummaryRepository summaryRepository;

    @Transactional
    public void save(Summary summary) {
        summaryRepository.save(summary);
    }
}