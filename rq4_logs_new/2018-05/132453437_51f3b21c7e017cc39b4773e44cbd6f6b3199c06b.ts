import Model from './model'

// class die furniture aanmaakt adhv een array
export default class Furniture {
    constructor(scene:any) {
        // furniture array (asset-name, x, y, z, rotation, scale)
        let furniture : [string, number, number, number, number, number][] = [
            ['kitchenFridge', 0.4, 0, -13.2, 180, 0.2],
            ['kitchenCoffeeMachine', 2.2, 0.83, -13.2, 180, 0.2],
            ['kitchenCabinet', 2, 0, -13.2, 180, 0.2]
        ]
        
        // loop door furniture array en maak de models
        furniture.forEach(element => {
            let item = new Model(scene, element[0], element[1], element[2], element[3], element[4], element[5])
        });
    }
}