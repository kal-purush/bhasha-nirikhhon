interface ITrucksResponse{
    trucks: Array<ITruckProfile>;
}

interface ITruckProfile {
    id:string;
    name:string;
    imageUrl:string;
    keywords:Array<string>;
    primaryColor:string;
    secondaryColor:string;
}

class TruckProfile implements ITruckProfile{
    id:string;
    name:string;
    imageUrl:string;
    keywords:Array<string>;
    primaryColor:string;
    secondaryColor:string;
}

interface IActiveTruck {
    id: string;
    latitude: number;
    longitude: number;
}

interface IActiveTrucksResponse {
    trucks: Array<IActiveTruck>;
}