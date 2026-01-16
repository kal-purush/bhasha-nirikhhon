const htmlToElement = (html) => {
    const template = document.createElement("template");
    html = html.trim(); // Never return a text node of whitespace as the result
    template.innerHTML = html;
    return template.content.firstChild;
  };
  
  const loadData = (path, callback) => {
    const xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = ({ target }) => {
      if (target.readyState == 4 && target.status == 200) {
        callback(target.responseXML);
      }
    };
    xhttp.open("GET", path, true);
    xhttp.send();
  };
  
  const generateTableRow = (item) => {
    const id = Array.from(item.getElementsByTagName(`id`))[0];
    const first_name = Array.from(item.getElementsByTagName(`first_name`))[0];
    const last_name = Array.from(item.getElementsByTagName(`last_name`))[0];
    const email = Array.from(item.getElementsByTagName(`email`))[0];
    const gender = Array.from(item.getElementsByTagName(`gender`))[0];
    const ip_address = Array.from(item.getElementsByTagName(`ip_address`))[0];
  
    return `<tr>
      <td>${id.textContent}</td>
      <td>${first_name.textContent}</td>
      <td>${last_name.textContent}</td>
      <td>${email.textContent}</td>
      <td>${gender.textContent}</td>
      <td>${ip_address.textContent}</td>
    </tr>`;
  };
  
  const renderTable = (xmlData) => {
    const table = document.getElementById("table-main");
  
    if (!table) {
      throw new Error("No table element found");
    }
  
    const nodes = Array.from(xmlData.documentElement.childNodes).filter(
      ({ nodeName }) => nodeName === `person`
    );
  
    nodes.map((personNode) =>
      table.appendChild(htmlToElement(generateTableRow(personNode)))
    );
  };
  
  loadData(`people.xml`, renderTable);