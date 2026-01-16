class Party {
    private vitality: number;
    private vitalityElement: HTMLDivElement;
    private goPartyElement: HTMLDivElement;
    private numberOfPartiesAttendedElement: HTMLDivElement;
    private timerToken: number;
    private numberOfPartiesAttended: number;

    constructor(vitalityElement: HTMLDivElement, goPartyElement: HTMLDivElement, numberOfPartiesAttendedElement: HTMLDivElement) {
        this.vitalityElement = vitalityElement;
        this.goPartyElement = goPartyElement;
        this.numberOfPartiesAttendedElement = numberOfPartiesAttendedElement;
        this.vitality = 0;
        this.numberOfPartiesAttended = 0;
        this.wireEvents();
    }

    wireEvents() {
        this.goPartyElement.addEventListener("click", () => {
            this.goParty();
        });
    }

    goParty() {
        if (this.vitality > 0) {
            this.vitality -= 100;
            this.updateVitality();
            this.numberOfPartiesAttended++;
            this.numberOfPartiesAttendedElement.innerHTML = this.numberOfPartiesAttended.toString();
        }
    }

    updateVitality() {
        this.vitalityElement.innerHTML = this.vitality.toString();
    }

    start() {
        this.timerToken = setInterval(() => this.increaseVitality(), 100);
    }

    increaseVitality() {
        this.vitality++;
        this.updateVitality();
    }
}

window.onload = () => {
    var vitalityTracker = <HTMLDivElement>document.getElementById("vitality");
    var goPartyElement = <HTMLDivElement>document.getElementById("goParty");
    var numberOfPartiesAttended = <HTMLDivElement>document.getElementById("numberOfPartiesAttended");
    var party = new Party(vitalityTracker, goPartyElement, numberOfPartiesAttended);
    party.start();
}