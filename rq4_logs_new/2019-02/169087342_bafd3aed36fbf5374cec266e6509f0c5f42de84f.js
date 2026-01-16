const doubleUp = document.querySelector(".container");
doubleUp.addEventListener('dblclick', e => {
    window.alert("So Close! Click on the 'Sign Up' Button below :)");
});

class Why {
    //constructor
    constructor (domElement) {

        this.domElement = domElement;

        this.expandButton = this.domElement.querySelector(".expandButton");

        this.expandButton.textContent = "Learn More";

        this.expandButton.addEventListener( 'click', () => this.expandWhy());


    }

    //method
    expandWhy() {
        this.domElement.classList.toggle('why-open');
    }


}




let whys = document.querySelectorAll(".why-indy").forEach( why => new Why(why));