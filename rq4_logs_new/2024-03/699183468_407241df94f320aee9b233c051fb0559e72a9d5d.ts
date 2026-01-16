import { createAsyncThunk, createSlice } from "@reduxjs/toolkit"
import { type SkillCategory, type SkillCategoryFilters } from "@custom-types/skill-category-type"
import { Loading } from "@custom-types/loadingType"
import { axiosInstance } from "@utils/axios-instance"
import { type AxiosError } from "axios"
import { type ApiError } from "@custom-types/apiErrorType"

export const getAllSkillCategories = createAsyncThunk(
  "skillCategories/getAll",
  async (_, thunkApi) => {
    try {
      const response = await axiosInstance.get("/admin/skill-categories/all")
      return response.data
    } catch (error) {
      const axiosError = error as AxiosError
      const response = axiosError.response?.data as ApiError
      return thunkApi.rejectWithValue(response.message)
    }
  }
)

export const getSkillCategories = createAsyncThunk(
  "skillCategories/getSkillCategories",
  async (params: SkillCategoryFilters | undefined, thunkApi) => {
    try {
      const response = await axiosInstance.get("/admin/skill-categories", {
        params,
      })
      return response.data
    } catch (error) {
      const axiosError = error as AxiosError
      const response = axiosError.response?.data as ApiError
      return thunkApi.rejectWithValue(response.message)
    }
  }
)

export const sortSkillCategories = createAsyncThunk(
  "skillCategory/sortSkillCategories",
  async (
    data: {
      skillCategories: SkillCategory[]
    },
    thunkApi
  ) => {
    try {
      const response = await axiosInstance.put(`/admin/skill-categories/sort`, data)
      return response.data
    } catch (error) {
      const axiosError = error as AxiosError
      const response = axiosError.response?.data as ApiError
      return thunkApi.rejectWithValue(response.message)
    }
  }
)

interface InitialState {
  loading: Loading.Idle | Loading.Pending | Loading.Fulfilled | Loading.Rejected
  loading_sort: Loading.Idle | Loading.Pending | Loading.Fulfilled | Loading.Rejected
  error: string | null
  skill_categories: SkillCategory[]
}

const initialState: InitialState = {
  loading: Loading.Idle,
  loading_sort: Loading.Idle,
  error: null,
  skill_categories: [],
}

const skillCategoriesSlice = createSlice({
  name: "app",
  initialState,
  reducers: {
    setSkillCategories: (state, action) => {
      state.skill_categories = action.payload
    },
  },
  extraReducers(builder) {
    /**
     * List all active
     */
    builder.addCase(getAllSkillCategories.pending, (state) => {
      state.loading = Loading.Pending
      state.error = null
    })
    builder.addCase(getAllSkillCategories.fulfilled, (state, action) => {
      state.loading = Loading.Fulfilled
      state.error = null
      state.skill_categories = action.payload
    })
    builder.addCase(getAllSkillCategories.rejected, (state, action) => {
      state.loading = Loading.Rejected
      state.error = action.payload as string
    })
    /**
     * List skill categories
     */
    builder.addCase(getSkillCategories.pending, (state) => {
      state.loading = Loading.Pending
      state.error = null
    })
    builder.addCase(getSkillCategories.fulfilled, (state, action) => {
      state.loading = Loading.Fulfilled
      state.error = null
      state.skill_categories = action.payload
    })
    builder.addCase(getSkillCategories.rejected, (state, action) => {
      state.loading = Loading.Rejected
      state.error = action.payload as string
    })
    /**
     * Sort skill categories
     */
    builder.addCase(sortSkillCategories.pending, (state) => {
      state.loading_sort = Loading.Pending
      state.error = null
    })
    builder.addCase(sortSkillCategories.fulfilled, (state) => {
      state.loading_sort = Loading.Fulfilled
      state.error = null
    })
    builder.addCase(sortSkillCategories.rejected, (state, action) => {
      state.loading_sort = Loading.Rejected
      state.error = action.payload as string
    })
  },
})

export const { setSkillCategories } = skillCategoriesSlice.actions
export default skillCategoriesSlice.reducer