
//for types we need to make unique strings for each action type
//every action we send technically goes threough every single reducer
//if we have two different actions called the same thing we will get werid behaviour
export const clickerTypes = {
    CLICKER_UPDATE_CLICKS: 'CLICKER_UPDATE_CLICKS'
}


//the number of clicks to go up or down by
export function updateClicks(clickDelta:number){
    //this object we return is the action
    return {
        //it has the mandatory type
        type:clickerTypes.CLICKER_UPDATE_CLICKS,
        //payload is for holding the new data
        payload:{
            clickDelta
        }
    }
}