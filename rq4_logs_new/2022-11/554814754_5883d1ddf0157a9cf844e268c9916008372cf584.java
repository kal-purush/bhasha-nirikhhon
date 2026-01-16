package project.TadeM.Admin.Dto;


import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import project.TadeM.Admin.Entity.Category;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CategoryDto {

	Long id;
	String categoryName;
	int sortValue;
	boolean usingYn;

	public static List<CategoryDto> of(List<Category> categories) { // 목록 구현
		if (categories != null) {
			List<CategoryDto> categoryList = new ArrayList<>();
			for (Category x : categories) {
				categoryList.add(of(x));
			}
			return categoryList;
		}
		return null;
	}

	public static CategoryDto of(Category category) {
		return CategoryDto.builder()
			.id(category.getId())
			.categoryName(category.getCategoryName())
			.sortValue(category.getSortValue())
			.usingYn(category.isUsingYn())
			.build();
	}
}