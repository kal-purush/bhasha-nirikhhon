export const mutations = {
  createProduct:  `mutation createProduct($createproductinput: CreateProductInput!) {
    createProduct(input: $createproductinput) {
      id
      name
      quility
    }
  }`,

  createBook: `mutation book($input: CreateBookInput!) {
    createBook(input: $input) {
      id
      price
      rating
      title
      content
    }
  }
  `
}