import { createAsyncThunk, createSlice } from "@reduxjs/toolkit"
import { type Skill } from "@custom-types/skill-type"
import { Loading } from "@custom-types/loadingType"
import { axiosInstance } from "@utils/axios-instance"
import { type AxiosError } from "axios"
import { type ApiError } from "@custom-types/apiErrorType"
import { type SkillFormData } from "@custom-types/form-data-type"

export const getSkill = createAsyncThunk("skill/getSkill", async (id: number, thunkApi) => {
  try {
    const response = await axiosInstance.get(`/admin/skills/${id}`)
    return response.data
  } catch (error) {
    const axiosError = error as AxiosError
    const response = axiosError.response?.data as ApiError
    return thunkApi.rejectWithValue(response.message)
  }
})

export const createSkill = createAsyncThunk(
  "skill/createSkill",
  async (data: SkillFormData, thunkApi) => {
    try {
      const response = await axiosInstance.post("/admin/skills", data)
      return response.data
    } catch (error) {
      const axiosError = error as AxiosError
      const response = axiosError.response?.data as ApiError
      return thunkApi.rejectWithValue(response.message)
    }
  }
)

export const updateSkill = createAsyncThunk(
  "skill/updateSkill",
  async (
    data: {
      id: number
      skillCategory: SkillFormData
    },
    thunkApi
  ) => {
    try {
      const response = await axiosInstance.put(`/admin/skills/${data.id}`, data.skillCategory)
      return response.data
    } catch (error) {
      const axiosError = error as AxiosError
      const response = axiosError.response?.data as ApiError
      return thunkApi.rejectWithValue(response.message)
    }
  }
)

export const deleteSkill = createAsyncThunk("skill/deleteSkill", async (id: number, thunkApi) => {
  try {
    const response = await axiosInstance.delete(`/admin/skills/${id}`)
    return response.data
  } catch (error) {
    const axiosError = error as AxiosError
    const response = axiosError.response?.data as ApiError
    return thunkApi.rejectWithValue(response.message)
  }
})

interface InitialState {
  loading: Loading.Idle | Loading.Pending | Loading.Fulfilled | Loading.Rejected
  error: string | null
  skill: Skill | null
  hasPreviousPage: boolean
  hasNextPage: boolean
  totalPages: number
  totalItems: number
  selectedSkillId: number | null
}

const initialState: InitialState = {
  loading: Loading.Idle,
  error: null,
  skill: null,
  hasPreviousPage: false,
  hasNextPage: false,
  totalPages: 0,
  totalItems: 0,
  selectedSkillId: null,
}

const skillCategorySlice = createSlice({
  name: "app",
  initialState,
  reducers: {
    setSelectedSkillId: (state, action) => {
      state.selectedSkillId = action.payload
    },
  },
  extraReducers(builder) {
    /**
     * Get skill
     */
    builder.addCase(getSkill.pending, (state) => {
      state.loading = Loading.Pending
      state.error = null
    })
    builder.addCase(getSkill.fulfilled, (state, action) => {
      state.loading = Loading.Fulfilled
      state.error = null
      state.skill = action.payload
    })
    builder.addCase(getSkill.rejected, (state, action) => {
      state.loading = Loading.Rejected
      state.error = action.payload as string
    })
    /**
     * Create
     */
    builder.addCase(createSkill.pending, (state) => {
      state.loading = Loading.Pending
      state.error = null
    })
    builder.addCase(createSkill.fulfilled, (state, action) => {
      state.loading = Loading.Fulfilled
      state.error = null
      state.skill = action.payload
    })
    builder.addCase(createSkill.rejected, (state, action) => {
      state.loading = Loading.Rejected
      state.error = action.payload as string
    })
    /**
     * Update
     */
    builder.addCase(updateSkill.pending, (state) => {
      state.loading = Loading.Pending
      state.error = null
    })
    builder.addCase(updateSkill.fulfilled, (state, action) => {
      state.loading = Loading.Fulfilled
      state.error = null
      state.skill = action.payload
    })
    builder.addCase(updateSkill.rejected, (state, action) => {
      state.loading = Loading.Rejected
      state.error = action.payload as string
    })
    /**
     * Delete
     */
    builder.addCase(deleteSkill.pending, (state) => {
      state.loading = Loading.Pending
      state.error = null
    })
    builder.addCase(deleteSkill.fulfilled, (state) => {
      state.loading = Loading.Fulfilled
      state.error = null
    })
    builder.addCase(deleteSkill.rejected, (state, action) => {
      state.loading = Loading.Rejected
      state.error = action.payload as string
    })
  },
})

export const { setSelectedSkillId } = skillCategorySlice.actions
export default skillCategorySlice.reducer