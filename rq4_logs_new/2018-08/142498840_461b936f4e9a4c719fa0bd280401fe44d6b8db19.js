// Datas
var general = {
    labels: ['Work', 'Study', 'Hobby', 'Exercise', 'Life', 'Social', 'Entertainment', 'School', 'Productivity', 'Sleep'],
    datasets: [{
        label: "General",
        backgroundColor: ['#AEC7E8', '#FFBB78', '#98DF8A', '#FF9896', '#C5B0D5', '#C49C94', '#F7B6D2', '#C7C7C7', '#DBDB8D', '#9EDAE5'],
        borderColor: '#313131',
        borderWidth: 2,
        hoverBorderWidth: 4,
        data: [403, 234, 138, 51, 94, 55, 90, 19, 35, 653],
    }]
};
var work = {
    labels: ['Waitering','Volunteering'],
    datasets: [{
        label: "Work",
        backgroundColor: ['#AEC7E8', '#AEC7E8'],
        data: [373, 30],
        borderColor: '#313131',
        borderWidth: 2,
        hoverBorderWidth: 4,
    }]
}

var canvas = document.getElementById('myChart');
var ctx = canvas.getContext('2d');
var chart = new Chart(ctx, {
    type: 'pie',
    data: general,
    options: {
        legend: {
            display: true,
            position: 'bottom',
            labels: {
                fontColor: 'white'
            }
        },
        tooltips: {
            callbacks: {
                afterLabel: function () {
                    return "hours";
                }
            }
        }
    }
});

canvas.onclick = function(evt) {
    var activePoints = chart.getElementsAtEvent(evt);
    if(activePoints[0]) {
        var name = general.labels[activePoints[0]['_index']];
        console.log(name);
        if(name == "Work") {
            changeData(work);
        }

    }
}

function changeData(newData) {
    console.log(newData);
    chart.data = newData;
    console.log(chart.data);
    chart.update();
}

