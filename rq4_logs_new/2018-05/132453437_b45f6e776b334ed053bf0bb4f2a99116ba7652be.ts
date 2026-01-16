export default class Assets {
    constructor(scene : any) {
        let assetsArray : Array<String> = [
            "kitchenBar",
            "kitchenBarEnd",
            "kitchenBlender",
            "kitchenCabinet",
            "kitchenCabinetCornerInner",
            "kitchenCabinetCornerRound",
            "kitchenCabinetDrawer",
            "kitchenCabinetUpper",
            "kitchenCabinetUpperCorner",
            "kitchenCabinetUpperDouble",
            "kitchenCabinetUpperLow",
            "kitchenCoffeeMachine",
            "kitchenFridge",
            "kitchenFridgeBuiltIn",
            "kitchenFridgeSmall",
            "kitchenMicrowave",
            "kitchenSink",
            "kitchenStove",
            "kitchenStoveElectric"
        ]

        let assets = document.createElement("a-assets");
        let path = "models/furniture/"
        for(var key in assetsArray) {
            let obj = this.createAsset(assetsArray[key], "obj", path)
            assets.appendChild(obj)

            let mtl = this.createAsset(assetsArray[key], "mtl", path)
            assets.appendChild(mtl)
        }
        
        scene.appendChild(assets)
    }

    createAsset(key: String, sort: String, path: String){
        let el = document.createElement("a-asset-item")
        let elName = key + "-" + sort;
            
        el.setAttribute("id", elName)
        el.setAttribute("src", path + "" + key + "." + sort)
        
        return el;
    }
}