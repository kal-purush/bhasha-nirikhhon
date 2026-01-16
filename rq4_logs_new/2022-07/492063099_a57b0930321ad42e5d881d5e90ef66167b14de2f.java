package ma.nttsquad.nttecomcore.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ma.nttsquad.nttecomcore.dto.CategoryByLangDto;
import ma.nttsquad.nttecomcore.exception.NttNotFoundException;
import ma.nttsquad.nttecomcore.mapper.CategoryByLangMapper;
import ma.nttsquad.nttecomcore.model.repository.CategoryByLangRepository;
import ma.nttsquad.nttecomcore.service.CategoryByLangSrv;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Service
public class CategoryByLangSrvImpl implements CategoryByLangSrv {

    CategoryByLangRepository categoryByLangRepository;

    @Override
    public List<CategoryByLangDto> getAllCategoriesByLang() {
        return categoryByLangRepository.findAll()
                .stream()
                .map(CategoryByLangMapper.INSTANCE::entityToDto)
                .collect(Collectors.toList());
    }

    @Override
    public CategoryByLangDto getCategoryById(Long categoryByLang_id) {
        log.trace("{}", categoryByLang_id);
        return categoryByLangRepository.findById(categoryByLang_id)
                .map(CategoryByLangMapper.INSTANCE::entityToDto)
                .orElseThrow(() -> new NttNotFoundException("There's no category with the id '%d'".formatted(categoryByLang_id)));

    }

    @Override
    public void saveCategoryByLang(CategoryByLangDto categoryByLangDto) {
        log.trace("start save category: {}",categoryByLangDto);
        categoryByLangRepository.save(CategoryByLangMapper.INSTANCE.dtoToEntity(categoryByLangDto));
        log.trace("end save category");
    }

    @Override
    public void updateCategoryByLang(Long categoryByLang_id, CategoryByLangDto categoryByLangDto) {
        log.trace("start update category: {} {}",categoryByLang_id, categoryByLangDto);
        CategoryByLangDto category = getCategoryById(categoryByLang_id);
        categoryByLangDto.setId(category.getId());
        categoryByLangRepository.save(CategoryByLangMapper.INSTANCE.dtoToEntity(categoryByLangDto));
        log.trace("end update category: {}",categoryByLang_id);
    }

    @Override
    public void deleteCategoryByLang(Long categoryByLang_id) {
        log.trace("start delete category: {}",categoryByLang_id);
        categoryByLangRepository.deleteById(categoryByLang_id);
        log.trace("end delete category: {}",categoryByLang_id);
    }
}