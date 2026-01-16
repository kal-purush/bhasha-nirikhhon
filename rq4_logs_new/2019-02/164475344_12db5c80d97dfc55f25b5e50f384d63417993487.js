function cell(rowNum, cellNum) {
    var even = true
    if (cellNum % 2 == 1) even = false
    if (even) {
        return `
        <div id="cell-1-${cellNum}" class="cell black">
            <div class="checker red-checker"></div>
        </div>        
        `
    } else
        return `<div id="cell-1-${cellNum}" class="cell red"></div>`
}

function rowTemplate() {
    var rowString = ''
    for (var cellNum=1; cellNum<=8; cellNum++) {
        rowString = rowString + cell(1, cellNum)
    }
    return rowString
}