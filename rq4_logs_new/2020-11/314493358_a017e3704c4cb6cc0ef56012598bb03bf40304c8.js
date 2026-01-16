class List {
    constructor(parentDiv, data) {
        this.parentDiv = parentDiv;
        this.data = data;

        // Feliratkozás
        window.addEventListener("checkAll", (event) => {
            this.OnCheckAll(event);
        });
        window.addEventListener("deleteChecked", () => {
            this.OnDeleteCheck();
        });
        window.addEventListener("addNew", (event) => {
            this.OnAddNew(event);
        });

        this.Render();
    }

    //Eseménykezelők
    OnCheckAll(event) {
        let checkValue = event.detail;

        this.data.map(element => element.checked = checkValue);

        this.Render();
    }

    OnDeleteCheck() {
        this.data = this.data.filter(element => !element.checked);

        this.Render();
    }

    OnAddNew(event){
        let newTask = event.detail;

        this.data.push({
            checked: false,
            task: newTask
        });

        this.Render()
    }

    //Függvények
    Render() {
        this.parentDiv.innerHTML = "";
        this.parentDiv.innerText = "";

        let newUl = document.createElement("ul");
        this.data.forEach((element, i) => {
            let newLi = document.createElement("li");

            let newCheckbox = document.createElement("input");
            newCheckbox.setAttribute("type", "checkbox");
            newCheckbox.id = `ch${i}`;
            newCheckbox.checked = element.checked;
            newCheckbox.addEventListener("change", () => {
                element.checked = newCheckbox.checked;
            });

            let newLabel = document.createElement("label");
            newLabel.setAttribute("for", newCheckbox.id);
            newLabel.innerText = element.task;

            newLi.appendChild(newCheckbox);
            newLi.appendChild(newLabel);
            newUl.appendChild(newLi);
        });

        this.parentDiv.appendChild(newUl);
    }
}