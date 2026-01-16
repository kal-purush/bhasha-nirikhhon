"use strict";
var Firework;
(function (Firework) {
    function generateContent(_titelList) {
        let group = null;
        let fieldset = document.querySelector("fieldset#fireworkLoad");
        group = createSelect(_titelList);
        if (fieldset && group)
            fieldset.appendChild(group);
    }
    Firework.generateContent = generateContent;
    function createSelect(_titelList) {
        let selection = document.createElement("select");
        selection.name = "LoadedTitels";
        selection.addEventListener("change", Firework.getDataFromServer);
        for (let titel of _titelList) {
            let option = document.createElement("option");
            option.setAttribute("name", titel.fireworkName);
            option.value = option.textContent = titel.fireworkName;
            selection.appendChild(option);
        }
        return selection;
    }
})(Firework || (Firework = {}));
//# sourceMappingURL=generateContent.js.map