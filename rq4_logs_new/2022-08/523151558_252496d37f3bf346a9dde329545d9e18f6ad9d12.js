const divContainer = document.getElementById('divContainer');




//Using javascript, create 256 (16 x 16) square divs

function createDivsForGrid(rows, columns) {
    for (i = 0 ;i < rows; i++) {
        const row = document.createElement('div');
        row.classList.add('gridRow');
        row.style.display = 'flex';
        for (j = 0 ;j < columns; j++) {
            const gridDiv =  document.createElement('div');
            gridDiv.style.width = '20px';
            gridDiv.style.height = '20px';
            gridDiv.style.border = '1px solid black';
            gridDiv.classList.add('gridDiv');
            gridDiv.style.display = 'flex';
            
            row.appendChild(gridDiv);
            
        }
        divContainer.appendChild(row);
    }
}

createDivsForGrid(16, 16);






//Figure out a way to use the 'hover' event to make the 
//divs change color as the mouse hovers over them 