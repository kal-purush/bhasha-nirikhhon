import Model from './model'
import {InvalidParamException} from '@/config/exception/common'
import { FailToGetShopeeBrandsException } from '@/config/exception/shopee'
import { TypedRequestQuery } from '@/utils/requestHandler'
import { ProductListResponse } from '@/config/types/shopee'
import axios from 'axios'


export default async function getShopeeFilterOptions(inputData:TypedRequestQuery<{q:string, brand:string}>):Promise<ProductListResponse | any>{
    try {
        let result 

        const model = new Model(inputData)

        //Execute API query
        try {
            const url = model.processURL()
            if(url === ""){
                return new InvalidParamException()
            }
            const {data} = await model.getProductList(url)
            result = model.getBrandOptions(data)

        } catch (error) {
           console.error(error)
           throw new FailToGetShopeeBrandsException()
        }

        return result

    } catch (error) {
        return error
    }
}