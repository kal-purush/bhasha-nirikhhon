import {createApi,fetchBaseQuery} from '@reduxjs/toolkit/query/react'
import { PizzaApi, SearchPizzaParams } from '../store/types'
import { createAsyncThunk } from '@reduxjs/toolkit';
import axios from 'axios';

import pickBy from 'lodash/pickBy';
import identity from 'lodash/identity';
//https://631aef4edc236c0b1ee7b8c6.mockapi.io/items?page=1&sortBy=price&order=desc&search=%D0%B1%D1%83%D1%80
export const fetchPizzas = createAsyncThunk<PizzaApi[], SearchPizzaParams>(
    'pizza/fetchPizzasStatus',
    async (dateOfHomeParams) => {
      const { sortBy, order, category, search, currentPage } = dateOfHomeParams;
      const { data } = await axios.get<PizzaApi[]>(`https://631aef4edc236c0b1ee7b8c6.mockapi.io/items`, {
        params: pickBy(
          {
            page: currentPage,
            //  limit: 3,
            category:category,
            sortBy:sortBy,
            order:order,
            search:search
          },
          identity,
        ),
      });
      return data;
    },
  );
  
//!under development for switch
  // export const fetchPizzas=createApi({
//     reducerPath:'api/pizzas',
//     baseQuery:fetchBaseQuery({baseUrl:'https://631aef4edc236c0b1ee7b8c6.mockapi.io'}),
//     endpoints:build=>({
//         getProducts:build.query<PizzaApi[], string>({query:
//             (all)=>({      
//                 url:`/items?${all}`,
//         })      
//         })
//     })
// })
// export const {useGetProductsQuery}=fetchPizzas

//TODO: 
//?-1) test 3 fetch in the function 
//?-2)switch to RTK query 
//?-3)Pagination