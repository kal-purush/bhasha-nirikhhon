import { PayloadAction, createAsyncThunk, createSlice } from '@reduxjs/toolkit'
import { HttpService } from '../../api/HttpService'
import {
  GetPermissionResponse,
  Permission,
  PermissionRequest,
} from '../../model/permission/permission'
import { ApiPathEnum } from '../../api/ApiPathEnum'
import { CommonResponse } from '../../model/common/common-response'

interface PermissionStateProps {
  permissions: Permission[]
  error: string
  loading: boolean
  pageSize: number
  pageCurrent: number
  totalPage: number
  totalItem: number
}

interface Meta {
  current: number
}

const PAGE_SIZE = import.meta.env.VITE_PAGE_SIZE
const initialState: PermissionStateProps = {
  permissions: [],
  error: '',
  loading: true,
  pageSize: PAGE_SIZE,
  pageCurrent: 0,
  totalPage: 0,
  totalItem: 0,
}
const { authHttpService } = new HttpService()

export const getPermissions = createAsyncThunk(
  'permission/getPermissions',
  async (data: Meta, thunkAPI) => {
    try {
      const response = await authHttpService.get<GetPermissionResponse>(
        ApiPathEnum.Permission,
        {
          params: {
            current: data.current,
            pageSize: PAGE_SIZE,
          },
          signal: thunkAPI.signal,
        },
      )

      return response.data
    } catch (error) {
      return thunkAPI.rejectWithValue(error)
    }
  },
)

export const createPermission = createAsyncThunk(
  'permission/createPermission',
  async (data: PermissionRequest, thunkAPI) => {
    try {
      const response = await authHttpService.post<CommonResponse<Permission>>(
        ApiPathEnum.Permission,
        data,
        {
          signal: thunkAPI.signal,
        },
      )

      if (response.status !== 201) {
        throw new Error(response.data.message)
      }

      return response.data
    } catch (error) {
      return thunkAPI.rejectWithValue(error)
    }
  },
)

export const updatePermission = createAsyncThunk(
  'permission/updatePermission',
  async (data: PermissionRequest, thunkAPI) => {
    try {
      const response = await authHttpService.patch<CommonResponse<Permission>>(
        `${ApiPathEnum.Permission}/${data._id}`,
        data,
        {
          signal: thunkAPI.signal,
        },
      )

      if (response.status !== 200) {
        throw new Error(response.data.message)
      }

      return response.data
    } catch (error) {
      return thunkAPI.rejectWithValue(error)
    }
  },
)

export const deletePermission = createAsyncThunk(
  'permission/deletePermission',
  async (id: string, thunkAPI) => {
    try {
      const response = await authHttpService.delete<CommonResponse<Permission>>(
        `${ApiPathEnum.Permission}/${id}`,
        {
          signal: thunkAPI.signal,
        },
      )

      if (response.status !== 200) {
        throw new Error(response.data.message)
      }

      return id
    } catch (error) {
      return thunkAPI.rejectWithValue(error)
    }
  },
)

const permissionSlice = createSlice({
  name: 'permission',
  initialState,
  reducers: {},
  extraReducers(builder) {
    builder.addCase(getPermissions.pending, state => {
      state.loading = true
    })
    builder.addCase(
      getPermissions.fulfilled,
      (state, action: PayloadAction<GetPermissionResponse>) => {
        state.loading = false
        state.pageCurrent = action.payload.data.meta.current
        state.pageSize = action.payload.data.meta.pageSize
        state.totalPage = action.payload.data.meta.pages
        state.totalItem = action.payload.data.meta.total
        state.permissions = action.payload.data.result
      },
    )
    builder.addCase(createPermission.pending, state => {
      state.loading = true
    })

    builder.addCase(
      createPermission.fulfilled,
      (state, action: PayloadAction<CommonResponse<Permission>>) => {
        state.permissions.push(action.payload.data)
        state.loading = false
      },
    )
    builder.addCase(createPermission.rejected, (state, action) => {
      state.loading = false
      state.error = action.payload as string
    })
    builder.addCase(updatePermission.pending, state => {
      state.loading = true
    })
    builder.addCase(
      updatePermission.fulfilled,
      (state, action: PayloadAction<CommonResponse<Permission>>) => {
        const permission = state.permissions
        const idx = permission.findIndex(x => x._id === action.payload.data._id)

        state.permissions[idx] = action.payload.data
        state.loading = false
      },
    )
    builder.addCase(updatePermission.rejected, (state, action) => {
      state.loading = false
      state.error = action.payload as string
    })
    builder.addCase(deletePermission.pending, state => {
      state.loading = true
    })
    builder.addCase(deletePermission.fulfilled, (state, action) => {
      state.permissions = state.permissions.filter(
        x => x._id !== action.payload,
      )
      state.loading = false
    })
    builder.addCase(deletePermission.rejected, (state, action) => {
      state.loading = false
      state.error = action.payload as string
    })
  },
})

const permissionReducer = permissionSlice.reducer

export default permissionReducer