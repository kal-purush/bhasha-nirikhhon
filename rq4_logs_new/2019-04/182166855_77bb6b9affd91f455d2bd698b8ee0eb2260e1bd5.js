/*eslint-env browser*/

var $ = function (id) {
    "use strict";
    return window.document.getElementById(id);
};

window.addEventListener("load", function () {
    "use strict";
    var Jukebox = function () {
        var albums = [], self;
        self = {
            addAlbum: function (album) {
                albums.push(album);
            },
            favoriteAlbum: function () {
                var max = -1, fav, i;
                for (i = 0; i < albums.length; i += 1) {
                    if (albums[i].played > max) {
                        max = albums[i].played;
                        fav = albums[i];
                    }
                }
                return fav.display();
            }
        };
        return self;
    };

    var Album = function (artist, title) {
        var self = {
            artist: artist,
            title: title,
            played: 0,
            play: function () {
                self.played += 1;
                $("favoriteAlbum").innerHTML = "Now playing: " + self.title + " by " + self.artist;
            },
            display: function () {
                return title + " by " + artist + ". This album has been played " + this.played + " times!";
            }
        };
        return self;
    };

    var jbox = new Jukebox();
    var albums = [["Operation Ivy", "Energy"], ["Blink 182", "Dude Ranch"], ["New Found Glory", "Sticks and Stones"]];
    var i, album, albumInstance;
    var albumCollection = [];
    var select = $("albums");
        
    
    for (i = 0; i < albums.length; i += 1) {
        album = document.createElement("option");
        album.textContent = albums[i][0] + " : " + albums[i][1];
        album.value = i;
        select.appendChild(album);
        albumInstance = new Album(albums[i][0], albums[i][1]);
        jbox.addAlbum(albumInstance);
        albumCollection.push(albumInstance);
    }
    
    $("play").addEventListener("click", function () {
       
        var i = parseInt(select.value, 10);
        if (isNaN(i)) {
            window.alert("You have to select an album, Silly!");
        } else {
            albumCollection[i].play();
        }
    });
    
    $("show").addEventListener("click", function () {
        $("favoriteAlbum").innerHTML = "Your favorite album is: " + jbox.favoriteAlbum();
    });
});