const listContainer = document.querySelector('#bodytable')



let list = []
async function loadTable() {
    const result = await axios.get('http://localhost:3000/crypto')
    list = result.data
    
    list.forEach(function (row, index) {
        const tr = document.createElement("tr")
        tr.innerHTML = `
        <td>${index + 1}</td>
        <td>${row.name}</td>
        <td>${row.base_unit}</td>
        <td>${row.buy}</td>
        <td>${row.sell}</td>
        <td>${row.last}</td>
        <td>${row.volume}</td>
        `
        listContainer.appendChild(tr)
    })
}
loadTable()

function startReverseTimer(seconds) {
    var timerElement = document.getElementById('timer');

    function updateTimer() {
        timerElement.innerHTML = seconds;

        if (seconds > 0) {
            seconds--;
        } else {
            seconds = 60;
            listContainer.innerHTML = ""
            loadTable()
            axios.post('http://localhost:3000/cryptodb')
        }
        setTimeout(updateTimer, 1000); 
        drawCircularProgressBar(seconds / 60)
    }

    updateTimer(); 
}


startReverseTimer(60);

function drawCircularProgressBar(progress) {
    var canvas = document.getElementById('progressCanvas');
    var context = canvas.getContext('2d');
    var centerX = canvas.width / 2;
    var centerY = canvas.height / 2;
    var radius = 15;
    var startAngle = -Math.PI / 2; 
    var endAngle = startAngle + (progress * 2 * Math.PI);

    
    context.clearRect(0, 0, canvas.width, canvas.height);

    
    context.beginPath();
    context.arc(centerX, centerY, radius, 0, 2 * Math.PI, false);
    context.lineWidth = 3;
    context.strokeStyle = '#ddd';
    context.stroke();

    
    context.beginPath();
    context.arc(centerX, centerY, radius, startAngle, endAngle, false);
    context.lineWidth = 3;
    context.strokeStyle = '#3DC6C1'; 
    context.stroke();


}





function toggleSwitch() {
    var switchElement = document.getElementById("toggleSwitch");
    switchElement.classList.toggle("toggle-on");
}

const middlebutton = document.querySelector(".middlebutton")

const bigtext = document.querySelector(".bigtext")
function updatePrice(event) {
    middlebutton.innerHTML = event.target.innerHTML
    const objList = list.find(obj => obj.base_unit === event.target.innerHTML.toLowerCase())
    console.log(objList)
    if (objList != null)
        bigtext.innerHTML = objList.buy


}

