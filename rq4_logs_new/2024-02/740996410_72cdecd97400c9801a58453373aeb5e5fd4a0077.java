package kr.co.teacherforboss.service.examService;

import java.util.List;
import kr.co.teacherforboss.domain.ExamCategory;
import kr.co.teacherforboss.domain.enums.Status;
import kr.co.teacherforboss.repository.ExamCategoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ExamQueryServiceImpl implements ExamQueryService {
    private final ExamCategoryRepository examCategoryRepository;

    @Override
    @Transactional(readOnly = true)
    public List<ExamCategory> getExamCategories() {
        return examCategoryRepository.findExamCategoriesByStatus(Status.ACTIVE);
    }

}