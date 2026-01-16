import {
    createAsyncThunk,
    createEntityAdapter,
    createSlice,
} from "@reduxjs/toolkit";
import { DataTable, DataGDTT } from "../../models/modelTableHNX";
import agent from "../../api/agent";
import { TableParams } from "../../models/modelLinkTable";
import { RootState } from "../../store/configureStore";

interface TableState {
    productsLoaded: boolean;
    ListDataTable: DataTable[];
    productParams: TableParams;
    status: string;
    index : number;
    floor:  string ;
    NameFloor :  string ;
    DataPt : DataTable[];
    DataBi : DataGDTT[]
}
type params = {
  Floor :string,
  Query:string
}
const dataTableAdapter = createEntityAdapter<DataTable>({
    selectId: (dataTable) => dataTable.RowID || '', // Chỉ định trường khóa
  });
export const getDataTable = createAsyncThunk("table/getDataTable" , async (Param :params)=>{
    try {
      console.log(Param.Query)
      if(Param.Query === "thoa-thuan-hsx"){
            const DataBi = await agent.dataGDTTtable.listBi(Param.Floor)
            const DataPt = await agent.dataGDTTtable.listPt(Param.Floor)
            let data = {
                index : 1,
                floor :"GDTT",
                NameFloor : Param.Floor,
                product : {
                  DataBi,
                  DataPt : DataPt  
                }  
             }
            
             return  data
      }
      else if(Param.Query === "danh-muc"){
        const DataHNX = await agent.ListDataTable.list("hnx" , "s=quote&l=HNXIndex")
        const DataHSX = await agent.ListDataTable.list("hsx" , "s=quote&l=HNXIndex")
        let data = {
          index : 0,
          floor :"Danh-muc",
          NameFloor : Param.Floor,
          product : DataHNX
        }

        return data   
   
      }
      else{  
            const result = await agent.ListDataTable.list(Param.Floor , Param.Query)
            let data = {
              index : 0,
              floor :"MAIN",
              NameFloor : Param.Floor,
              product : result
            }

            return data         
        }
    } catch (error) {
        console.log('lỗi table silce', error);
    }

})

function initParams() {
    return {
        s: "quote",
        l: "HNXIndex",
    };
}

export const tableDanhMucSlice = createSlice({
    name: "table",
    initialState:dataTableAdapter.getInitialState<TableState>({
        productsLoaded: false,
        ListDataTable: [] as  DataTable[],
        status: "idle",
        index : 0,
        floor : 'MAIN',
        DataBi :  [] as DataGDTT[],
        DataPt :  [] as DataTable[],
        NameFloor : "",
        productParams: initParams(),
    }),
    reducers: {
        setProductParams: (state, action) => {
            state.productsLoaded = false;
            state.productParams = { ...state.productParams, ...action.payload };
        },
    },

    extraReducers: (builder) => {
        builder
          .addCase(getDataTable.pending , (state, action) =>{
            state.productsLoaded = false;
            state.status = "loading";
          })
          .addCase(getDataTable.fulfilled , (state, action) =>{
            state.productsLoaded = true;
            state.status = "idle";
            let data = action.payload
            if(data?.index === 0){
              if(data.NameFloor === "HNX"){
                data?.product.map((obj:DataTable) =>
                      obj.Info?.sort((a: any, b: any) => {
                        const indexA = Number(a[0]);
                        const indexB = Number(b[0]);
                        if (indexA < indexB) {
                          return -1;
                        }
                        if (indexA > indexB) {
                          return 1;
                        }
                        return 0;
                      }) )
                     state.floor = "MAIN"
                     state.ListDataTable = data?.product
                     state.NameFloor = data?.NameFloor
              }else{
                state.floor = "MAIN"
                state.ListDataTable = data?.product
                state.NameFloor = data?.NameFloor
              }
            }
            else{
              if(data?.NameFloor === "HSX"){
                state.DataPt = data.product?.DataPt?.PUT_EXEC
                state.DataBi = data.product?.DataBi
                state.floor = "GDTT" 
                state.NameFloor = "HSX"
              }else{
                state.DataPt = data?.product?.DataPt
                state.DataBi = data?.product?.DataBi
                state.floor = "GDTT"
                state.NameFloor = "HNX"
              }
         
            }
          })
          .addCase(getDataTable.rejected , (state, action) =>{

          })
    },
});

export default tableDanhMucSlice;
export const {setProductParams} = tableDanhMucSlice.actions;