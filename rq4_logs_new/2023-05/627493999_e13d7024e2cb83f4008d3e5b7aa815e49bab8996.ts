import { createSlice } from '@reduxjs/toolkit'
import type { PayloadAction } from '@reduxjs/toolkit'
import type { RootState } from '../../app/store'
import { Product } from '../../../types'

// Define a type for the slice state
interface ProductsState {
  products: Product[]
}

// Define the initial state using that type
const initialState: ProductsState = {
  products: [],
}

//hold all products in state
//for search purposes 
//getAllProducts: () => make Fetch request to API and store all items in a products array state (WHY? bc making API calls are expensive)
//getProduct: (payload) => returns a type of product
    //getWomensClothing: () => filter all products and retuern to UI ana rray of womens clothing
    //getMensClothing: () => filter all products and return to UI an array with mens clothing
    //getElectronics: () => filter all products and return 

export const productSlice = createSlice({
  name: 'products',
  // `createSlice` will infer the state type from the `initialState` argument
  initialState,
  reducers: {
    getAllProducts: (state: ProductsState = initialState, action: PayloadAction<string>): ProductsState => {
      // let newState = [...state.products]; //[...initialState.products, state]
      // newState.push(action.payload);
      fetch('https://fakestoreapi.com/products')
            .then(res => res.json())
            .then(items => {
              state.products = [...items];
            })
            .catch(err => console.log(err));
      return state;
    },
     getProductCategory: (state: ProductsState = initialState, action: PayloadAction<string>): Product[] => {
        switch(action.payload) {
          case "Women's Clothing":
            //filter womens clothing
            //return array w/ women's clothing
            const filteredProducts = state.products.filter(product => product.category === "women's clothing");
            break;
          case "Men's Clothing":
            //filter mens clothing
            //return array w/ mens clothing
            break;
          case "Electronics":
            //filter electronics
            //return array w/ electronics
            break;
          case "Jewelry":
            //filter jewlwery
            //return array w/ jewelry
            break;
          default:
            //handle invalid choice , tell user it is invalid
            return null;
        }
    },
    getProductSearch: (state: ProductsState = initialState, action: PayloadAction<string>) => {
        let filtered = state.products.filter(filterSearchTerm)
    }   

  },
})

export const { getAllProducts } = productSlice.reducer

// Other code such as selectors can use the imported `RootState` type
// export const selectShoppingCart = (state: RootState) => state.shoppingCart.products;

// export default productSlice.reducer

//function that filters according to appearance of search term in title
const filterSearchTerm = (products: Product[], searchTerm : string) =>{
    let filteredArray = [];
    for(let i = 0; i < products.length; i++){
        let newProduct = products.title;
        if(newProduct.search(/searchTerm/i) !== -1){
            filteredArray.push(products[i])
        }
    }
    return filteredArray;
}