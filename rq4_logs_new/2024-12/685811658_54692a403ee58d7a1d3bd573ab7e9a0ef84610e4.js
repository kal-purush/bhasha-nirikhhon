import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import axios from "axios";

const location = window.location;

let url = '';

if(location?.hostname?.toLowerCase() === "localhost" || location?.hostname?.toLowerCase() === "zen"){
    url = "http://zen/jo/api-lib/App/Factory_DashBoard";
}else{
    url = "http://zen/jo/api-lib/App/Factory_DashBoard";
}


export const fetchFactoryData = createAsyncThunk('factory/Summary_Purchase', async() => {
        try {

            const body = {
                "Token" : "9065471700535651"  
                ,"ReqData":"[{\"Token\":\"9065471700535651\",\"Evt\":\"Summary_Purchase\",\"FDate\":\"12/30/2024\",\"TDate\":\"12/30/2024\",\"MetalTypeId\":\"2\",\"CategoryId\":\"1\"}]"
              }

            const response = await axios.post(url, body);
            return response;
        } catch (error) {
            console.log(error);
            throw new Error(error.response ? error.response.data : error.message);
        }
});

export const fetchMaster = createAsyncThunk('fetch/Master', async() => {
        try {
            const body = {
                "Token" : "9065471700535651"  
                ,"ReqData":"[{\"Token\":\"9065471700535651\",\"Evt\":\"Master\"}]"
              }

            const response = await axios.post(url, body);
            return response;
        } catch (error) {
            console.log(error);
            throw new Error(error.response ? error.response.data : error.message);
        }
});

export const fetchSummary_SaleData = createAsyncThunk('factory/Summary_Sale', async() => {
        try {
            const body = {
                "Token" : "9065471700535651"  
                ,"ReqData":"[{\"Token\":\"9065471700535651\",\"Evt\":\"Summary_Sale\",\"FDate\":\"12/30/2024\",\"TDate\":\"12/30/2024\",\"MetalTypeId\":\"2\",\"CategoryId\":\"1\"}]"
              }

            const response = await axios.post(url, body);
            return response;
        } catch (error) {
            console.log(error);
            throw new Error(error.response ? error.response.data : error.message);
        }
});

export const fetchVendor_Margin_Per_CaratData = createAsyncThunk('factory/Vendor_Margin_Per_Carat', async() => {
        try {
            const body = {
                "Token" : "9065471700535651"  
                ,"ReqData":"[{\"Token\":\"9065471700535651\",\"Evt\":\"Vendor_Margin_Per_Carat\",\"FDate\":\"12/30/2024\",\"TDate\":\"12/30/2024\",\"MetalTypeId\":\"2\",\"CategoryId\":\"1\"}]"
              }

            const response = await axios.post(url, body);
            return response;
        } catch (error) {
            console.log(error);
            throw new Error(error.response ? error.response.data : error.message);
        }
});

export const fetchVendor_In_Out_DurationData = createAsyncThunk('factory/Vendor_In_Out_Duration', async() => {
        try {
            const body = {
                "Token" : "9065471700535651"  
                ,"ReqData":"[{\"Token\":\"9065471700535651\",\"Evt\":\"Vendor_In_Out_Duration\",\"FDate\":\"12/30/2024\",\"TDate\":\"12/30/2024\",\"MetalTypeId\":\"2\",\"CategoryId\":\"1\"}]"
              }

            const response = await axios.post(url, body);
            return response;
        } catch (error) {
            console.log(error);
            throw new Error(error.response ? error.response.data : error.message);
        }
});

export const FactoryApi = createSlice({
  name:'factory',
  initialState:{
    Summary_Purchase : {
        loading:false,
        data:null,
        error:null
    },
    Master : {
        loading:false,
        data:null,
        error:null
    },
    Summary_Sale:{
        loading:false,
        data:null,
        error:null
    },
    Vendor_Margin_Per_Carat:{
        loading:false,
        data:null,
        error:null
    },
    Vendor_In_Out_Duration:{
        loading:false,
        data:null,
        error:null
    }
  },
  reducers: {},
  extraReducers:(builder) => {
        builder
        .addCase(fetchFactoryData.pending, (state) => {
            state.Summary_Purchase.loading = true;
            state.Summary_Purchase.data = null;
            state.Summary_Purchase.error = null;
        })
        .addCase(fetchFactoryData.fulfilled, (state, action) => {
            state.Summary_Purchase.loading = false;
            state.Summary_Purchase.data = action.payload;
            state.Summary_Purchase.error = null;
        })
        .addCase(fetchFactoryData.rejected, (state, action) => {
            state.Summary_Purchase.loading = false;
            state.Summary_Purchase.data = [];
            state.Summary_Purchase.error = action.error.message;
        })

        .addCase(fetchMaster.pending, (state) => {
            state.Master.loading = true;
            state.Master.data = null;
            state.Master.error = null;
        })
        .addCase(fetchMaster.fulfilled, (state, action) => {
            state.Master.loading = false;
            state.Master.data = action?.payload;
            state.Master.error = null;
        })
        .addCase(fetchMaster.rejected, (state, action) => {
            state.Master.loading = false;
            state.Master.data = null;
            state.Master.error = action.error.message;
        })

        .addCase(fetchSummary_SaleData.pending, (state) => {
            state.Summary_Sale.loading = true;
            state.Summary_Sale.data = null;
            state.Summary_Sale.error = null;
        })
        .addCase(fetchSummary_SaleData.fulfilled, (state, action) => {
            state.Summary_Sale.loading = false;
            state.Summary_Sale.data = action?.payload;
            state.Summary_Sale.error = null;
        })
        .addCase(fetchSummary_SaleData.rejected, (state, action) => {
            state.Summary_Sale.loading = false;
            state.Summary_Sale.data = null;
            state.Summary_Sale.error = action.error.message;
        })

        .addCase(fetchVendor_Margin_Per_CaratData.pending, (state) => {
            state.Vendor_Margin_Per_Carat.loading = true;
            state.Vendor_Margin_Per_Carat.data = null;
            state.Vendor_Margin_Per_Carat.error = null;
        })
        .addCase(fetchVendor_Margin_Per_CaratData.fulfilled, (state, action) => {
            state.Vendor_Margin_Per_Carat.loading = false;
            state.Vendor_Margin_Per_Carat.data = action?.payload;
            state.Vendor_Margin_Per_Carat.error = null;
        })
        .addCase(fetchVendor_Margin_Per_CaratData.rejected, (state, action) => {
            state.Vendor_Margin_Per_Carat.loading = false;
            state.Vendor_Margin_Per_Carat.data = null;
            state.Vendor_Margin_Per_Carat.error = action.error.message;
        })

        .addCase(fetchVendor_In_Out_DurationData.pending, (state) => {
            state.Vendor_In_Out_Duration.loading = true;
            state.Vendor_In_Out_Duration.data = null;
            state.Vendor_In_Out_Duration.error = null;
        })
        .addCase(fetchVendor_In_Out_DurationData.fulfilled, (state, action) => {
            state.Vendor_In_Out_Duration.loading = false;
            state.Vendor_In_Out_Duration.data = action?.payload;
            state.Vendor_In_Out_Duration.error = null;
        })
        .addCase(fetchVendor_In_Out_DurationData.rejected, (state, action) => {
            state.Vendor_In_Out_Duration.loading = false;
            state.Vendor_In_Out_Duration.data = null;
            state.Vendor_In_Out_Duration.error = action.error.message;
        })
  }
})

export default FactoryApi?.reducer;