import { createAction } from "@reduxjs/toolkit"
import { ItemsPropsDog } from "./type"

export const fetchDataAdminItems = createAction<ItemsPropsDog>('dogwatcher/FetchDataDog')