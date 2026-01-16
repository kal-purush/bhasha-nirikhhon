interface CategoriesAndSubCategoriesInterface {
  [key: string]: string[];
}

const categoriesAndSubCategories: CategoriesAndSubCategoriesInterface = {
  clothes: [
    "beachwear",
    "coats and jackets",
    "hoodies and sweatshirts",
    "jeans",
    "shorts",
    "sportswear",
    "suits",
  ],
  shoes: ["brogues", "flats", "heels", "loafers", "trainers"],
};

// CONCAT ALL SUB_CATEGORIES WITH A LOOP
const allSubCategories: string[] = [];
// LOOP THE OBJECT AND PUSH ALL VALUES TO ARRAY TO GATHER THEM ALL IN A SINGLE ARRAY
Object.keys(categoriesAndSubCategories).forEach((key) => {
  allSubCategories.push(...categoriesAndSubCategories[key]);
});

export { categoriesAndSubCategories, allSubCategories };