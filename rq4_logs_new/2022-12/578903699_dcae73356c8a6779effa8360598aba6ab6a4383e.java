package com.example.controller;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.example.domain.Original;
import com.example.repository.OriginalRepository;

@Controller
@RequestMapping("/original")
public class InsertController {
	@Autowired
	private OriginalRepository repository;

	@GetMapping("")
	public String insert() {
		// tsvファイルのpathを指定
		String filePath = "/Users/shibatamasayuki/業務/大量データ課題/train.tsv";
		Map<String, Integer> categoryCountMap = new HashMap<>();
		List<String[]> tsvDataList = new ArrayList<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line;
			Original original = new Original();

			// tsvファイル1行目(カラム名の部分)は不必要なため、先に読み込んでおく
			line = br.readLine();

			while ((line = br.readLine()) != null) {
				String[] data = line.split("\t", 0);
				tsvDataList.add(data);
				
				String id = data[0];
				String name = data[1];
				String conditionId = data[2];
				String categoryName = data[3];
				String brand = data[4];
				String price = data[5];
				String shipping = data[6];
				
				try {
					String description = data[7];
					original.setDescription(description);
				} catch (ArrayIndexOutOfBoundsException e) {
					String description = "";
					original.setDescription(description);
				}

				int j = 1;

				while (j <= 7) {

					if (j == 1) {
						original.setId(Integer.parseInt(id));
					} else if (j == 2) {
						original.setName(name);
					} else if (j == 3) {
						original.setConditionId(Integer.parseInt(conditionId));
					} else if (j == 4) {
						original.setCategoryName(categoryName);
					} else if (j == 5) {
						original.setBrand(brand);
					} else if (j == 6) {
						original.setPrice(Double.parseDouble(price));
					} else if (j == 7) {
						original.setShipping(Integer.parseInt(shipping));
					}
					j++;
				}
				repository.insert(original);
//				System.out.println("original : " + original);
			}
			br.close();
			System.out.println("Data has been inserted successfully.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "result";
	}

}