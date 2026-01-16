import data from "./out.json" assert { type: "json" };

let contentStr = data.map(
	(item) => `
        <tr><td colspan=3>${item.input}</td></tr>

        <tr><th colspan=3>Classes</th></tr>
        ${item.classes.map((cls) => `<tr><td colspan=3>${cls}</td></tr>`).join("")}

        <tr><th colspan=3>Attributes</th></tr>
        <tr>
            <td><u>Name</u></td>
            <td colspan=2><u>Associated Class</u></td>
        </tr>
        ${item.attributes
					.map(
						(attr) => `
            <tr>
                <td>${attr.name}</td>
                <td colspan=2>${attr.class}</td>
            </tr>
        `
					)
					.join("")}
        
        <tr><th colspan=3>Relatiosnhips</th></tr>
        <tr>
            <td><u>Subject</u></td>
            <td><u>Predicate</u></td>
            <td><u>Object</u></td>
        </tr>
        ${item.relationships
					.map(
						(r) => `
            <tr>
                <td>${r.subject}</td>
                <td>${r.predicate}</td>
                <td>${r.object}</td>
            </tr>
        `
					)
					.join("")}
    `
);

document.getElementById("table").innerHTML = contentStr.join("<tr><td colspan=3></td></tr>");