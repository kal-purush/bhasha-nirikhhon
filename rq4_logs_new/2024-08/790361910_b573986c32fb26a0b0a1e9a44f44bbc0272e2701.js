const DEFAULT_API_BASE_URL = 'http://127.0.0.1:12345/api';
const DEFAULT_WIKI_DATA_MD = 'New Item';


function wiki_itemList_getAll() {
    //console.log("[DEBUG] wiki_itemList_getAll() pressed.");    
    let url = DEFAULT_API_BASE_URL + '/items';
    fetch(url)
    .then(response => response.json())
    .then(data => {
        //console.log('Respone Json: ', data);
        let e = document.getElementById('divPageContent');
        if(data.status != 200) {
            e.innerHTML = '<span class="errorText">[ERROR] Failed to load data.<span>';
            e.innerHTML += '<br><br>';
            e.innerHTML += '<span class="errorText jsonData">' + JSON.stringify(data) + '</span>';
            e.innerHTML += '<br><br>';
        } else {
            e.innerHTML = '<u>List of Wiki Pages (id, page_content)</u>: &nbsp;\n';
            e.innerHTML += '<br>\n';
            if(Object.keys(data.items).length < 1) {
                e.innerHTML += '<i>N/A - no items were found in list</i> <br>\n';
            } else {
                for (let [key, value] of Object.entries(data.items)) {
                    e.innerHTML += '<span class="tBold">' + key + ': &nbsp; </span>';
                    let sValue = value.replace(/<br\s*>/gi, '\n'); // Remove line breaks so it displays properly
                    e.innerHTML += '<span class="tData">' + sValue + '</span>';
                    e.innerHTML += '<br>';
                }
            }
        }        
    })
    .catch(err => console.log(err));    
}


function wiki_new_showForm() {
    //console.log("[DEBUG] wiki_edit_markdown() pressed.");
    e = document.getElementById('divPageContent');
    e.innerHTML = `<table>
    <tr>        
        <td>
            <button id="btnCancel" onClick="wiki_itemList_getAll();">Cancel</button>
        </td>
        <td>
            <button id="btnUpdateMarkdown" onClick="wiki_new();">Save Changes</button>
        </td>
    </tr>
    `;
    e.innerHTML += '<br><br>';
    e.innerHTML += '<textarea id="taMarkdownCode" name="taMarkdownCode" rows=25>' + DEFAULT_WIKI_DATA_MD + '</textarea>\n';
    e.innerHTML += '<br>';      
}


function wiki_new() {
    //console.log("[DEBUG] load_markdown() pressed."); 
    let mdCode = document.getElementById('taMarkdownCode').value;
    //console.log('[DEBUG] mdCode:  ', mdCode);
    let url = DEFAULT_API_BASE_URL + '/items'; // Update the url
    fetch(url, 
        {
            method: 'POST',
            headers: {
                'Accept': 'application/json, text/plain, */*',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ data: mdCode })
        }
    )
    .then(response => response.json())
    .then(data => {
        //console.log('Respone Json: ', data);
        let e = document.getElementById('divPageContent');
        if(data.status != 200) {
            e.innerHTML = '<span class="errorText">[ERROR] Failed to create new wiki page.<span>';
            e.innerHTML += '<br><br>';
            e.innerHTML += '<span class="errorText jsonData">' + JSON.stringify(data) + '</span>';
            e.innerHTML += '<br><br>';
        } else {
            let e = document.getElementById('txtWikiId');
            e.value = data.id;
            wiki_html_get();
        }
    })
    .catch(err => console.log(err));    
}


function wiki_html_get() {
    //console.log("[DEBUG] wiki_html_get() pressed.");    
    let id = document.getElementById('txtWikiId').value;  // Get the id from the input box
    let url = DEFAULT_API_BASE_URL + '/md_to_html/' + id; // Update the url
    fetch(url)
    .then(response => response.json())
    .then(data => {
        //console.log('Respone Json: ', data);
        let e = document.getElementById('divPageContent');
        if(data.status != 200) {
            e.innerHTML = '<span class="errorText">[ERROR] Failed to load data.<span>';
            e.innerHTML += '<br><br>';
            e.innerHTML += '<span class="errorText jsonData">' + JSON.stringify(data) + '</span>';
            e.innerHTML += '<br><br>';
        } else {
            e.innerHTML = data.data;
        }        
    })
    .catch(err => console.log(err));    
}

function wiki_edit_markdown() {
    //console.log("[DEBUG] wiki_edit_markdown() pressed.");    
    let id = document.getElementById('txtWikiId').value;  // Get the id from the input box
    let url = DEFAULT_API_BASE_URL + '/items/' + id; // Update the url
    fetch(url)
    .then(response => response.json())
    .then(data => {
        //console.log('Respone Json: ', data);
        let e = document.getElementById('divPageContent');
        if(data.status != 200) {
            e.innerHTML = '<span class="errorText">[ERROR] Failed to load data.<span>';
            e.innerHTML += '<br><br>';
            e.innerHTML += '<span class="errorText jsonData">' + JSON.stringify(data) + '</span>';
            e.innerHTML += '<br><br>';
        } else {
            e.innerHTML = `<table>
    <tr>        
        <td>
            <button id="btnCancel" onClick="wiki_html_get();">Cancel</button>
        </td>
        <td>
            <button id="btnUpdateMarkdown" onClick="wiki_markdown_update();">Save Changes</button>
        </td>
    </tr>
    `;
            e.innerHTML += '<br><br>';
            e.innerHTML += '<textarea id="taMarkdownCode" name="taMarkdownCode" rows=25>' + data.items[id] + '</textarea>\n';
            e.innerHTML += '<br>';            
        }
    })
    .catch(err => console.log(err));    
}


function wiki_markdown_update() {
    //console.log("[DEBUG] load_markdown() pressed.");    
    let id = document.getElementById('txtWikiId').value;  // Get the id from the input box
    let mdCode = document.getElementById('taMarkdownCode').value;
    //console.log('[DEBUG] mdCode:  ', mdCode);
    let url = DEFAULT_API_BASE_URL + '/items/' + id; // Update the url
    fetch(url, 
        {
            method: 'POST',
            headers: {
                'Accept': 'application/json, text/plain, */*',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ data: mdCode })
        }
    )
    .then(response => response.json())
    .then(data => {
        //console.log('Respone Json: ', data);
        let e = document.getElementById('divPageContent');
        if(data.status != 200) {
            e.innerHTML = '<span class="errorText">[ERROR] Failed to update wiki page.<span>';
            e.innerHTML += '<br><br>';
            e.innerHTML += '<span class="errorText jsonData">' + JSON.stringify(data) + '</span>';
            e.innerHTML += '<br><br>';
        } else {
            wiki_html_get();
        }
    })
    .catch(err => console.log(err));    
}


function wiki_delete() {
    let answer = window.confirm('Delete Wiki page from the database?');
    //console.log('[DEBUG] User deletion:  ' + answer);
    if (answer) {
        let id = document.getElementById('txtWikiId').value;  // Get the id from the input box
        let url = DEFAULT_API_BASE_URL + '/items/' + id; // Update the url
        fetch(url, 
            {
                method: 'DELETE',
                headers: {
                    'Accept': 'application/json, text/plain, */*',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ id: id })
            }
        )
        .then(response => response.json())
        .then(data => {
            //console.log('Respone Json: ', data);
            let e = document.getElementById('divPageContent');
            if(data.status != 200) {
                e.innerHTML = '<span class="errorText">[ERROR] Failed to delete wiki page.<span>';
                e.innerHTML += '<br><br>';
                e.innerHTML += '<span class="errorText jsonData">' + JSON.stringify(data) + '</span>';
                e.innerHTML += '<br><br>';
            } else {
                wiki_itemList_getAll();
            }
        })
        .catch(err => console.log(err));    
    } else {
        // Do nothing
    }
}
