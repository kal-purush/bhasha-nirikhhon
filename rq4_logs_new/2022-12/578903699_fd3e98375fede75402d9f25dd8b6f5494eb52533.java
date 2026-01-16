package com.example.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.example.domain.Category;
import com.example.domain.Items;
import com.example.domain.Original;
import com.example.repository.CategoryRepository;
import com.example.repository.ItemsRepository;

/**
 * @author shibatamasayuki 
 * category tableとitems tableを操作するController.
 */
@Controller
@RequestMapping("")
public class InsertCategoryAndItemsController {
	@Autowired
	CategoryRepository categoryRepository;

	@Autowired
	ItemsRepository itemsRepository;

	Category category = new Category();
	Items items = new Items();

	// 大カテゴリーと大カテゴリーのidを含んだHashMap
	Map<String, Integer> bigCategoryCountMap = new HashMap<>();
	// 中カテゴリーと中カテゴリーのidを含んだHashMap
	Map<String, Integer> mediumCategoryCountMap = new HashMap<>();
	// 中カテゴリーとcount(null=DB未追加 1=DB追加済み)を含んだHashMap
	Map<String, Integer> smallCategoryCountMap = new HashMap<>();

	@GetMapping("")
	public void insertCategory(Original original) {
		String categoryName = original.getCategoryName();

		// e.g. ["Men", "Clothes", "Jeans"]
		String[] categoryData = categoryName.split("/", 0);
		// e.g. "Men"
		String bigCategory = categoryData[0];
		// e.g. "Men/Clothes"
		String mediumCategory = categoryData[0] + "/" + categoryData[1];
		// e.g. "Men/Clothes/Jeans"
		String smallCategory = categoryData[0] + "/" + categoryData[1] + "/" + categoryData[2];

		// 一度もDBに追加していない大カテゴリーのDB追加を行う
		if (bigCategoryCountMap.get(bigCategory) == null) {
			category.setParent(null);
			category.setName(categoryData[0]);
			category.setNameAll(null);
			// 大カテゴリーを追加する
			categoryRepository.insert(category);

			// 大カテゴリーのidを取得
			Integer parentId = categoryRepository.findBigCategoryIdByName(categoryData[0]);
			// HashMapにカテゴリー名、大カテゴリーのidを追加
			bigCategoryCountMap.put(bigCategory, parentId);
		}

		// 一度もDBに追加していない中カテゴリーのDB追加を行う
		if (mediumCategoryCountMap.get(mediumCategory) == null) {
			// 大カテゴリーのidをset
			category.setParent(bigCategoryCountMap.get(categoryData[0]));
			category.setName(categoryData[1]);
			category.setNameAll(null);
			// 中カテゴリーを追加する
			categoryRepository.insert(category);

			// 追加したばかりの中カテゴリーのidを取得
			Integer parentId = categoryRepository.findMediumCategoryIdByName(categoryData[1],
					bigCategoryCountMap.get(categoryData[0]));
			// HashMapにカテゴリー名、中カテゴリーのidを追加
			mediumCategoryCountMap.put(mediumCategory, parentId);
		}

		// 一度もDBに追加していない小カテゴリーをcategory tableに追加、またitemsテーブルも追加
		if (smallCategoryCountMap.get(smallCategory) == null) {
			Integer count = 1;
			// 小カテゴリーと紐づいている中カテゴリーのidをset
			category.setParent(mediumCategoryCountMap.get(mediumCategory));
			category.setName(categoryData[2]);
			category.setNameAll(smallCategory);
			// 小カテゴリーを追加する
			categoryRepository.insert(category);

			// 追加したばかりの小カテゴリーのidを取得
			Integer parentId = categoryRepository.findSmallCategoryIdByName(categoryData[2],
					mediumCategoryCountMap.get(mediumCategory));
			smallCategoryCountMap.put(smallCategory, count);

			// items Domainに各データをset、itemsテーブルに追加
			items.setName(original.getName());
			items.setCondition(original.getConditionId());
			items.setCategory(parentId);
			items.setBrand(original.getBrand());
			items.setPrice(original.getPrice());
			items.setShipping(original.getShipping());
			items.setDescription(original.getDescription());
			itemsRepository.insert(items);

		}

	}
}