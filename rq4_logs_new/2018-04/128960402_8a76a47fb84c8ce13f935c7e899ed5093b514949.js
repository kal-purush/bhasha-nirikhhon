(function () {

    var celebration = document.getElementById('celebration');
    var leo = document.getElementById('leo-image');
    var happy = document.getElementById('happy');
    var bottom = document.getElementById('bottom');
    

    celebration.playbackRate = 0.5;
    
    setTimeout(() => {
        happy.classList.add('active')
        bottom.classList.add('active')
    }, 5500)

    setTimeout(() => {
        leo.classList.add('active')
    }, 1000)
    
    



}())