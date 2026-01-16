package cn.umbrella.api.enums;

import cn.umbrella.commons.enums.IGDCommonEnum;

public enum MemrType  implements IGDCommonEnum<Integer>{
	TOURIST(0, "游客"),//默认游客
	//TOURIST_SITE_CODE(SITE_CODE, "游客"),//其他游客
	PERSON(1, "个人"),
	ENTERPRISE(2, "企业");

	private int value;
	private String name;

	private MemrType(int value, String name) {
		this.value = value;
		this.name = name;
	}

	@Override
	public Integer getValue() {
		return this.value;
	}

	@Override
	public String getName() {
		return this.name;
	}

	public static String getNameByValue(Integer value) {
		for (IGDCommonEnum<Integer> item : values()) {
			if (item.getValue() == value) {
				return item.getName();
			}
		}
		return null;
	}

	public static Integer getValueByName(String name) {
		for (IGDCommonEnum<Integer> item : values()) {
			if (item.getName().equals(name)) {
				return (Integer) item.getValue();
			}
		}
		return null;
	}

	public static Integer getValueByItemName(String itemName) {
		Integer res = null;
		try {
			res = valueOf(itemName).getValue();
		} catch (IllegalArgumentException | NullPointerException e) {
		}
		return res;
	}
}