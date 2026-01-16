class CurrentConditionsDisplay implements IObserver, IDisplayElement {
    private _weatherData: ISubject;

    private _temperature: number;
    private _humidity: number;

    constructor(weatherData: ISubject) {
        this._weatherData = weatherData;
        this._weatherData.registerObserver(this);
    }

    update(temperature: number, humidity: number, pressure: number): void {
        this._temperature = temperature;
        this._humidity = humidity;
        this.display();
    }

    display(): void {
        console.log('現在の気象状況：温度' + this._temperature + '度  湿度' + this._humidity + '%');
    }
}