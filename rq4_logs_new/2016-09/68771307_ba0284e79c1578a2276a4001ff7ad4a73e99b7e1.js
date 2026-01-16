var torrentlist = [
    {
        'title': 'O Lobo de Wall Street',
        'magnet': 'magnet:?xt=urn:btih:4b642d022980e5ebaa7cf4b6e1cc93769921cb42&dn=The+Wolf+of+Wall+Street+%282013%29+1080p+BrRip+x264+-+YIFY&tr=udp%3A%2F%2Ftracker.leechers-paradise.org%3A6969&tr=udp%3A%2F%2Fzer0day.ch%3A1337&tr=udp%3A%2F%2Fopen.demonii.com%3A1337&tr=udp%3A%2F%2Ftracker.coppersurfer.tk%3A6969&tr=udp%3A%2F%2Fexodus.desync.com%3A6969',
        'cover': 'images/wallstreet.jpg'
    }
];

for (let i = 0; i < torrentlist.length; i++){
    console.log(makelist(torrentlist[i].title, torrentlist[i].magnet, torrentlist[i].cover));
}

function makelist(title, magnet, cover){
    return `<span class="title">${title}</span>
    <span class="magnet">${magnet}</span>
    <img src="${cover}"></img>`
}