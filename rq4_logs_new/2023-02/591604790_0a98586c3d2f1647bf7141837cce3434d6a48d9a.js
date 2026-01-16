
const btn = document.getElementById('btn');
btn.addEventListener('click', addToList)

let list = localStorage.getItem('list')
if (JSON.parse(list)){
    var obj = JSON.parse(list)

}else{
    var obj = {}
}

for (let key in JSON.parse(list)){
    let li = document.createElement('li');
    let txt = document.createTextNode(JSON.parse(list)[key]);
    li.appendChild(txt)
    document.getElementById('list').appendChild(li)
}

function addToList(){
    let task = document.getElementById('input');
    let li = document.createElement('li');
    let txt = document.createTextNode(task.value);
    if (task.value !== ''){
        let uniq = 'id' + (new Date()).getTime(); 
        obj[uniq] = task.value;
        localStorage.setItem('list', JSON.stringify(obj));
        task.value = '';
        li.appendChild(txt)
        document.getElementById('list').appendChild(li)
    }
  

    
}
