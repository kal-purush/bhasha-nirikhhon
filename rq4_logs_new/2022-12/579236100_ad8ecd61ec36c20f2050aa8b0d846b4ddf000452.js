    
function render(str) {
    
    if (str == undefined) return

    let result = sqrm(str)
    
    let html,json;
    if (result.docs !== undefined && Array.isArray(result.docs)) {
        html = '';
        json = [];
        result.docs.forEach(r => {
            html += '\n' + r.html;
            json.push(r.json);
        });
    } else {
        html = result.html;
        json = result.json;
    }

    let sanitized = DOMPurify.sanitize(html);
    
    document.body.innerHTML = sanitized
    
    return json
}