const filter = () => {
    const input = document.getElementById('search');
    const filterValue = input.value.toUpperCase();
    const colorList = document.getElementById('color_list');
    const color = colorList.getElementsByTagName('li');

    for(i=0; i<color.length; i++){
        let filterColor = color[i].getElementsByTagName('a')[0];

        if(filterColor.innerHTML.toUpperCase().indexOf(filterValue) > -1){
            color[i].style.display = '';

        } else {
            color[i].style.display = 'none';
        }
    }
}

const changeColor = (event) => event.style.color = 'green';

const clearForm = () => document.getElementById('search').value = '';