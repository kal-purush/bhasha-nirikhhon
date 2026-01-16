package com.spartanullnull.otil.domain.category.service;

import static graphql.Assert.*;

import com.spartanullnull.otil.domain.category.entity.*;
import com.spartanullnull.otil.domain.category.repository.*;
import com.spartanullnull.otil.domain.user.entity.*;
import java.util.*;
import java.util.stream.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.test.autoconfigure.jdbc.*;
import org.springframework.boot.test.autoconfigure.orm.jpa.*;
import org.springframework.context.annotation.*;
import org.springframework.test.context.*;

@ActiveProfiles("test")
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({CategoryService.class, User.class, Category.class})
class CategoryServiceTest {

    @Autowired
    private CategoryService categoryService;

    @Autowired
    private CategoryRepository categoryRepository;

    public static Stream<Arguments> createCategories() {
        return Stream.of(
            Arguments.of(
                List.of(
                    new Category(2L, "java"),
                    new Category(3L, "python")
                )
            )
        );
    }

    @BeforeEach
    void setUp() {
        categoryRepository.save(
            new Category(1L, "java")
        );
    }

    @AfterEach
    void tearDown() {
        categoryRepository.deleteAll();
    }

    @ParameterizedTest
    @MethodSource("createCategories")
    @DisplayName("기존 카테고리가 존재한다면 필터링을 합니다.")
    public void 카테고리_필터링(List<Category> categories) {
        // WHEN
        List<Category> notExistingCategories = categoryService.getNotExistingCategories(categories);

        // THEN
        assertTrue(notExistingCategories.size() == 1);
    }
}