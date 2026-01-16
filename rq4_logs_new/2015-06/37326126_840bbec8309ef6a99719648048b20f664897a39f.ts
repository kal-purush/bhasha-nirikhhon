/**
 * Created by Pierre on 21/06/15.
 */
export class Deezer{
    url:String = 'https://api.deezer.com';
    urlUser:String = '/user';
    urlPlaylist:String = '/playlists';
    urlSearch:String = '/search?q=';

    searchPlaylist(search){
        let url:String = this.url+this.urlSearch+search;
        return new Promise((resolve, reject) => {
            //fetch(url)
            fetch('data/search.json')
            .then(response => response.json())
            .then(response => {
                    resolve(response); // resolve promise with response if it fetch succeded
                }).catch(() => {
                    reject(); // reject promise if we catch a fetch error
                });
        });
    }

    getPlaylist(pseudo = null){
        let url:String = this.url+this.urlUser+'/'+pseudo+this.urlPlaylist;
        return new Promise((resolve, reject) => {
            //fetch(url)
            fetch('data/playlist.json')
                .then(response => response.json())
                .then(response => {
                    resolve(response); // resolve promise with response if it fetch succeded
                }).catch(() => {
                    reject(); // reject promise if we catch a fetch error
                });
        });
    }
}