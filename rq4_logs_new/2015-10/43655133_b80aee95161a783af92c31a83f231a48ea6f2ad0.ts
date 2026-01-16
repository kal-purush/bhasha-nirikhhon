class MealReservation {
    formattedPrice: KnockoutComputed<string>;
    name: KnockoutObservable<string>;
    price: KnockoutObservable<number>;
    constructor(name: string, price: number) {
        this.name = ko.observable(name).extend({ binding: "textInput" });
        this.price = ko.observable(price);
        this.formattedPrice = ko.computed(() => (this.price() ? `$${Number(this.price.peek()).toFixed(2) }` : "None"), null, { deferEvaluation: true, pure: true });
    }
}
class SeatReservation {
    additions: KnockoutObservableArray<string>;
    bindings = { meal: "options:$root.availableMeals,optionsText:'name'" }
    meal: KnockoutObservable<MealReservation>;
    constructor(public name: string, initialMeal: MealReservation, additions?: string[]) {
        this.meal = ko.observable(initialMeal);
        this.additions = ko.observableArray(additions || []).extend({ bindings: "options:$root.additions,attr:{multiple:'multiple'}" });
    }
}
class ReservationsViewModel {
    additions = ["Baggage", "Skis", "Wheelchair"];
    addSeat: any;
    availableMeals = [
        new MealReservation("Standard (sandwich)", 0),
        new MealReservation("Premium (lobster)", 34.95),
        new MealReservation("Ultimate (whole zebra)", 290)
    ];
    count: KnockoutComputed<number>;
    removeSeat: (seat: SeatReservation) => void;
    seats = ko.observableArray([
        new SeatReservation("Steve", this.availableMeals[0]),
        new SeatReservation("Bert", this.availableMeals[1], this.additions.filter((item, index) => index !== 1))
    ]).extend({ bindings: { value: "attr:{title:'seats'}" } });
    showSurcharge: KnockoutComputed<boolean>;
    totalSurcharge: KnockoutComputed<string>;
    constructor() {
        this.count = ko.computed(() => this.seats().length);
        this.totalSurcharge = ko.computed(() => this.seats().reduce((total, seat) => (total + Number(seat.meal().price())), 0).toFixed(2));
        this.showSurcharge = ko.computed(() => this.totalSurcharge() !== "0.00").extend({ binding: "visible" });
        this.addSeat = ko.utils.extend(() => { this.seats.push(new SeatReservation("", this.availableMeals[0])); }, { bindings: "enable:seats().length<5" });
        this.removeSeat = seat => this.seats.remove(seat);
    }
}
var model = new ReservationsViewModel();
ko.bindingProvider["bindings"] = {
    content: "template: 'reservations'",
    seats: "visible:count",
    "additions-list": { bindings: "text:additions" },
    availableMeals: "attr:{title:'available meals'}"
};
ko.applyBindings(model, document.getElementById("content")); 