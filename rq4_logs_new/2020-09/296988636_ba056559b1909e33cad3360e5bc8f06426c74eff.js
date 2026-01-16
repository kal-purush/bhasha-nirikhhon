export const VIEW_DRUG = "view drug"
export function viewDrug(id){
    return {
        type: VIEW_DRUG,
        viewDrugID: id 
    }
}

export const UPDATE_LIBRARY = "update drug library"
export function updateDrugList(list){
    return {
        type: UPDATE_LIBRARY,
        drugs: list
    }
}

export const SET_DRUG_DETAILS = "set drug details"
export function setDrugDetails(drug){
    return {
        type: SET_DRUG_DETAILS,
        drug: drug 
    }
}