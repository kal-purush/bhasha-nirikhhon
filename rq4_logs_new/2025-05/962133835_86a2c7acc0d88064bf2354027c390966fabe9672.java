package ewm.categories.controller;

import ewm.categories.dto.CategoryDto;
import ewm.categories.dto.NewCategoryDto;
import ewm.categories.service.CatService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(path = "/admin/categories")
@RequiredArgsConstructor
@Slf4j
public class AdminCatController {
    private final CatService catService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CategoryDto postCat(@RequestBody @Valid @NotNull NewCategoryDto request) {
        return catService.postCat(request);
    }

    @DeleteMapping(path = "/{catId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteCat(@PathVariable @Positive @NotNull Long catId) {
        catService.deleteCat(catId);
    }

    @PatchMapping(path = "/{catId}")
    @ResponseStatus(HttpStatus.OK)
    public CategoryDto patchCat(@PathVariable @Positive @NotNull Long catId,
                         @RequestBody @Valid @NotNull NewCategoryDto catPatch) {
        return catService.patchCat(catId, catPatch);
    }
}