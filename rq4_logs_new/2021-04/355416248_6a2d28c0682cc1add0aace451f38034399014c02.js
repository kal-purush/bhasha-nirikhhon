let countdown = document.createElement('div')
countdown.style.fontSize = '50px';
countdown.style.background = '#FF9933';
countdown.style.margin = "50px 30%";
countdown.style.textAlign = 'center';
countdown.style.color = '#000080';
document.body.append(countdown);
countdown.innerText = 10;

setTimeout(() => {
    countdown.innerText = 9;
    setTimeout(() => {
        countdown.innerText = 8;
        setTimeout(() => {
            countdown.innerText = 7;
            setTimeout(() => {
                countdown.innerText = 6;
                setTimeout(() => {
                    countdown.innerText = 5;
                    setTimeout(() => {
                        countdown.innerText = 4;
                        setTimeout(() => {
                            countdown.innerText = 3;
                            setTimeout(() => {
                                countdown.innerText = 2;
                                setTimeout(() => {
                                    countdown.innerText = 1;
                                    setTimeout(() => {
                                        countdown.innerText = 'Happy Independence Day!'
                                    }, 1000);
                                }, 1000);
                            }, 1000)
                        }, 1000)
                    }, 1000)
                }, 1000)
            }, 1000)
        }, 1000)
    }, 1000)
}, 1000)