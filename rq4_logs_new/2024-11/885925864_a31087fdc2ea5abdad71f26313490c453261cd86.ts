import { PayloadAction, createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import { TConstructorIngredient, TIngredient, TOrder } from '@utils-types';
import { orderBurgerApi } from '@api';

type TCounstructorState = {
  constructorItems: {
    bun: TConstructorIngredient | null;
    ingredients: TConstructorIngredient[];
  };
  order: TOrder | null;
  isLoading: boolean;
};

const initialState: TCounstructorState = {
  constructorItems: {
    bun: null,
    ingredients: []
  },
  order: null,
  isLoading: false
};

export const orderBurger = createAsyncThunk(
  'order/placeOrder',
  async (data: string[]) => {
    const res = await orderBurgerApi(data);
    return res.order;
  }
);

export const constructorSlice = createSlice({
  name: 'burgerConstructor',
  initialState,
  reducers: {
    addIngredient: {
      prepare: (ingredient: TIngredient) => ({
        payload: { ...ingredient, id: crypto.randomUUID() }
      }),
      reducer: (state, { payload }: PayloadAction<TConstructorIngredient>) => {
        payload.type === 'bun'
          ? (state.constructorItems.bun = payload)
          : state.constructorItems.ingredients.push(payload);
      }
    },
    moveIngredientUp: (state, action) => {
      const ingredientIndex = state.constructorItems.ingredients.findIndex(
        (ingredient) => ingredient.id === action.payload.id
      );
      const previousElem =
        state.constructorItems.ingredients[ingredientIndex - 1];
      state.constructorItems.ingredients.splice(
        ingredientIndex - 1,
        2,
        action.payload,
        previousElem
      );
    },
    moveIngredientDown: (state, action) => {
      const ingredientIndex = state.constructorItems.ingredients.findIndex(
        (ingredient) => ingredient.id === action.payload.id
      );
      const nextElem = state.constructorItems.ingredients[ingredientIndex + 1];
      state.constructorItems.ingredients.splice(
        ingredientIndex,
        2,
        nextElem,
        action.payload
      );
    },
    removeIngredient: (state, { payload }: PayloadAction<number>) => {
      state.constructorItems.ingredients.splice(payload, 1);
    },
    resetOrder: (state) => {
      state.order = initialState.order;
    }
  },
  extraReducers: (builder) => {
    builder
      .addCase(orderBurger.pending, (state) => {
        state.isLoading = true;
      })
      .addCase(orderBurger.rejected, (state) => {
        state.isLoading = false;
      })
      .addCase(orderBurger.fulfilled, (state, action) => {
        state.isLoading = false;
        state.constructorItems.bun = initialState.constructorItems.bun;
        state.constructorItems.ingredients =
          initialState.constructorItems.ingredients;
        state.order = action.payload;
      });
  },
  selectors: {
    selectOrderLoading: (state) => state.isLoading,
    selectOrderModalData: (state) => state.order,
    selectOrder: (state) => state.constructorItems
  }
});

export const { selectOrderLoading, selectOrderModalData, selectOrder } =
  constructorSlice.selectors;
export const {
  addIngredient,
  removeIngredient,
  moveIngredientUp,
  moveIngredientDown,
  resetOrder
} = constructorSlice.actions;