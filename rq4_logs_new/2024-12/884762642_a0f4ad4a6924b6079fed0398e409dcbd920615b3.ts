import { useState } from 'react';

const useCategory = () => {
  const [selectedCategory, setSelectedCategory] = useState<string[]>(['정리/공간 활용']);

  const categoryMap: Record<string, number> = {
    '정리/공간 활용': 1,
    주방: 2,
    청소: 3,
    건강: 4,
    IT: 5,
    '뷰티&패션': 6,
    '여가&휴식': 7,
    로컬: 8,
    기타: 9,
  };

  const handleCategoryClick = (category: string) => {
    setSelectedCategory((prev) => {
      if (prev.includes(category)) {
        if (prev.length > 1) {
          return prev.filter((cat) => cat !== category);
        } else {
          return prev;
        }
      } else {
        return [...prev, category];
      }
    });
  };

  const categoryList = Object.keys(categoryMap);
  const selectedCategoryNumbers = selectedCategory.map((category) => categoryMap[category]);

  return {
    selectedCategory,
    handleCategoryClick,
    categoryList,
    selectedCategoryNumbers,
  };
};

export default useCategory;