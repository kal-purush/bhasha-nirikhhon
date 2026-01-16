export default function addAnimeOnStorage(title, tipo){
    if(localStorage.getItem(tipo) === null ||  localStorage.getItem(tipo) === ''){
      localStorage.setItem(tipo, title);
      tipo === 'watching' && localStorage.setItem('watchingEp', '1')
      tipo === 'ended' && localStorage.setItem('timesEnded', '1')
    }else{
      localStorage.setItem(tipo,  localStorage.getItem(tipo)+","+title);
      tipo === 'watching' && localStorage.setItem('watchingEp', localStorage.getItem('watchingEp')+',1')       
      tipo === 'ended' && localStorage.setItem('timesEnded', localStorage.getItem('timesEnded')+',1')       
    }
}