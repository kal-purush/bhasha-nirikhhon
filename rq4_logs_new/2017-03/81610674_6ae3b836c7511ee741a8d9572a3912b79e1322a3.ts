export interface ICar {
    name: string;
    plate: string;
}
export class Car implements ICar {
    public name: string;
    public plate: string;

    constructor(name: string, plate: string) {
        this.name = name;
        this.plate = plate;
    }

    public IsCollected() {
        return CarRetriever.GetCarStorage(this);
    }

    public SetCollected(collected: boolean) {
        CarRetriever.SetCarStorage(this, collected);
    }

    public getNameCapitalized() {
        return this.name.slice(0, 1).toUpperCase() + this.name.slice(1).toLowerCase();
    }

    public getPlateCapitalized() {
        return this.plate.slice(0, 1).toUpperCase() + this.plate.slice(1).toLowerCase();
    }

    public Search(query: string): boolean {
        if (query.trim() == "") return true;

        let removel33tspelling = query
            .replace("4", "a")
            .replace("3", "e")
            .replace("6", "g")
            .replace("9", "j")
            .replace("1", "l")
            .replace("0", "o")
            .replace("0", "q")
            .replace("5", "s")
            .replace("7", "t")
            .replace("2", "z");
        let l33tSearch = this.plate.toLowerCase().indexOf(removel33tspelling) != -1;
        let nameSearch = this.name.toLowerCase().indexOf(query.toLowerCase()) != -1;
        let plateSearch = this.plate.toLowerCase().indexOf(query.toLowerCase()) != -1;

        return l33tSearch || nameSearch || plateSearch;
    }

    public createKey() {
        return this.name + this.plate;
    }
}

export class Collection {

    public name: string;
    public cars: Car[];
    public color: string;

    constructor(name: string, cars: ICar[], color: string) {
        this.name = name;
        this.color = color;
        this.cars = cars.map(Collection.constructCar);
    }

    public IsCompletelyCollected(): boolean {
        return this.cars.every((car) => car.IsCollected());
    }

    public IsPartlyCollected(): boolean {
        return this.GetAmountCollected() > 0;
    }

    public GetAmountCollected() {
        return this.cars.filter((c) => c.IsCollected()).length
    }

    public GetTotalCars() {
        return this.cars.length;
    }

    public GetBonus() {
        return 20000 + (15000 * (this.GetTotalCars() - 2));
    }

    public GetBonusText() {
        const bonus = this.GetBonus().toString();
        return `${bonus.slice(0, 2)}.${bonus.slice(2, 9999)}`;
    }

    public Search(query: string): boolean {
        query = query.toLowerCase();
        let carSearch = this.cars.filter((c) => c.Search(query)).length > 0;
        let collectionSearch = this.name.toLowerCase().indexOf(query) != -1;

        return carSearch || collectionSearch;
    }

    private static constructCar(car: ICar): Car {
        return new Car(car.name, car.plate);
    }
}

export interface RefreshListener {
    onRefresh(collections: Collection[]): void;
}

export class CarRetriever {

    private static RefreshListeners: RefreshListener[] = [];

    static AddRefreshListener(refreshListener: RefreshListener) {
        this.RefreshListeners.push(refreshListener);
    }

    static RemoveRefreshListener(refreshListener: RefreshListener) {
        const index = this.RefreshListeners.indexOf(refreshListener);
        this.RefreshListeners = this.RefreshListeners.splice(index, 1);
    }

    static TriggerRefresh() {
        this.GetCollections().then((c) => {
            this.RefreshListeners.forEach((r) => r.onRefresh(c));
        });
    }

    static GetCollections(): JQueryPromise<Collection[]> {
        const result = $.Deferred<Collection[]>();
        const cars = $.get("./cars.json");
        cars.then((data: any) => {
            const parsed: Collection[] = [];
            data.collections.forEach((collection: any) => {
                parsed.push(new Collection(collection.name, collection.cars, collection.color));
            });
            result.resolve(parsed);
        });
        cars.fail(() => result.reject([]));

        return result.promise();
    }

    static GetCarStorage(cars: Car): boolean {
        return localStorage.getItem(cars.createKey()) == "true";
    }

    static SetCarStorage(car: Car, bool: boolean) {
        localStorage.setItem(car.createKey(), String(bool));
    }
}