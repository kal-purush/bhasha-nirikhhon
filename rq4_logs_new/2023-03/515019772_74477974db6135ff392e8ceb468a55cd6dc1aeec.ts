import { createSlice } from "@reduxjs/toolkit/dist";
import { createAsyncThunk } from "@reduxjs/toolkit/dist";
import { ResponseStatus } from "../../../types/Generales/ResponseStatus";
import { MoraXVendedor } from "../../../types/Reportes/Mora/MoraXVendedor";
import MoraService from "./MoraService";
interface MoraXVendedorYSupState extends ReduxState {
  MoraXVendedor: MoraXVendedor[];
  MoraStatus: ResponseStatus | null;
}

const initialState: MoraXVendedorYSupState = {
  MoraXVendedor: [],
  MoraStatus: null,
  isError: false,
  isSuccess: false,
  isLoading: false,
  message: "",
};

export const getMoraXVendedor = createAsyncThunk(
  "Reportes/MoraXVendedor",
  async (data, { rejectWithValue }) => {
    const result = await MoraService.getMoraXVendedor(data);
    if (Array.isArray(result)) {
      return result;
    } else {
      return rejectWithValue(result);
    }
  }
);

export const MoraSlice = createSlice({
  name: "MoraXVendedorYSup",
  initialState,
  reducers: {
    reset: (state) => {
      state.isLoading = false;
      state.isSuccess = false;
      state.isError = false;
      state.message = "";
      state.MoraXVendedor = [];
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(getMoraXVendedor.fulfilled, (state, action) => {
        state.isLoading = false;
        state.isSuccess = true;
        state.MoraXVendedor = action.payload;
      })
      .addCase(getMoraXVendedor.rejected, (state, action) => {
        state.isLoading = false;
        state.isError = true;
        state.MoraStatus = action.payload as ResponseStatus;
      })
      .addCase(getMoraXVendedor.pending, (state) => {
        state.isLoading = true;
      });
  },
});

export const { reset } = MoraSlice.actions;
export default MoraSlice.reducer;