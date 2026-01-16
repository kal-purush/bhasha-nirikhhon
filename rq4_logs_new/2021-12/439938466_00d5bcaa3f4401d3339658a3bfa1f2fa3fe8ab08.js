import fetch from 'node-fetch';
import Twit from 'twit';


const T = new Twit({
    consumer_key:         'igRm0TTwfdlFUaFP9cwL6toUv',
    consumer_secret:      'SVwSsmlFRxHtM7HVKxNmDZEVrrKkuK2U0MLYtbSzPHZGifjdGb',
    access_token:         '1423495553397235713-e6kvuhNSwArC0rSg0SwtJHVVJTKabx',
    access_token_secret:  'dvDEY6RTaT4SDS5EGws60Bla4GY4OMxy62NWdpCgMBDCf',
})


const url = 'https://api.quran.com/api/v4/verses/random?language=ar&words=false';

//  get aya by key
const getVerse =(key ,action)=>{
    fetch("https://api.quran.com/api/v4/quran/verses/imlaei?verse_key="+key)
        .then((response) => response.json())
        .then((data) =>action(data.verses[0].text_imlaei))
        .catch((err) => console.log(err));
}
// get verse-key then get the verse
const randomAya = (action)=>{
    fetch(url)
    .then((response) => response.json())
    .then((data) => getVerse(data.verse.verse_key ,action) )
    .catch((err) => console.log(err));

}
const tweetVerse =(aya)=>{
    T.post('statuses/update', { status: aya }, function(err, data, response) {
        console.log(data)
      })
}

setInterval(() => {
    randomAya(tweetVerse)
}, 5*60*1000);