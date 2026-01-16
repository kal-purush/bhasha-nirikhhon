package com.spartanullnull.otil.domain.category.service;

import com.spartanullnull.otil.domain.category.entity.*;
import com.spartanullnull.otil.domain.category.repository.*;
import com.spartanullnull.otil.domain.post.entity.*;
import java.util.*;
import java.util.function.*;
import lombok.*;
import org.springframework.stereotype.*;
import org.springframework.transaction.annotation.*;

@Service
@RequiredArgsConstructor
@Transactional
public class CategoryService {

    private final CategoryRepository categoryRepository;

    /**
     * 카테고리 문자열 리스트로부터 카테고리 엔티티 생성 및 저장
     *
     * @param categoryInputs 카테고리 입력 문자열들
     * @param post           게시글 엔티티
     * @return 게시글과 연관된 카테고리들 반환
     * @author 임지훈
     */
    public List<Category> buildAndSaveCategoriesByRequest(List<String> categoryInputs, Post post) {
        // 카테고리 문자열 리스트 -> 카테고리 엔티티 리스트
        List<Category> categories = getCategoriesByRequest(categoryInputs);

        // 기존 카테고리가 아닌 카테고리들에 대해 저장
        List<Category> notExistingCategories = categories.stream()
            .filter(category ->
                categoryRepository
                    .findByCategoryNameAndPost(category.getCategoryName(), post)
                    .isEmpty()
            ).toList();
        categoryRepository.saveAll(notExistingCategories);

        // Post와 연관관계 설정
        categories.forEach(
            category ->
                category.updatePost(post)
        );

        // 기존 + 새로 저장된 카테고리들 반환
        return categories;
    }

    /**
     * 입력문자열 리스트 로부터 카테고리 엔티티 리스트 생성
     *
     * @param categoryInputs 입력문자열 리스트
     * @return 카테고리 엔티티 리스트
     */
    private List<Category> getCategoriesByRequest(List<String> categoryInputs) {
        return categoryInputs.stream()
            .map(buildCategoryByRequest()
            ).toList();
    }

    /**
     * 입력문자열로부터 카테고리 엔티티 생성
     *
     * @return 카테고리 엔티티
     */
    private Function<String, Category> buildCategoryByRequest() {
        return categoryInput ->
            Category.builder()
                .categoryName(categoryInput)
                .build();
    }
//    /**
//     * 기존 카테고리를 제외한 새로운 카테고리 리스트 생성
//     *
//     * @param categoryInputs 카테고리 문자열들
//     * @param post           카테고리 관련 게시글
//     * @return 생성된 카테고리 엔티티들
//     * @author 임지훈
//     */
//    public List<Category> createNewCategoriesExcludingExisting(List<String> categoryInputs,
//        Post post) {
//        return categoryInputs
//            .stream()
//            .filter(isNotExistingCategory())
//            .map(buildCategoryByRequestInput())
//            .toList();
//    }
//
//    /**
//     * 주어진 카테고리 입력문자열을 통해 categoryRepository 에 저장된 기존 카테고리가 아닌지 검증
//     *
//     * @return 기존 카테고리에 대한 입력문자열이 아니라면 true, 이라면 false
//     * @author 임지훈
//     */
//    private Predicate<String> isNotExistingCategory() {
//        return categoryName ->
//            categoryRepository
//                .findByCategoryName(categoryName)
//                .isEmpty();
//    }
//

}