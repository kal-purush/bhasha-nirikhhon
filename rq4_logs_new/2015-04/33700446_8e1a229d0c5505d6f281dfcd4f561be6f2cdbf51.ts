class Playlist {
    //genres: Genre[] = [];
    videos: Video[] = [];

    //selectedGenre: string = "";

    /**
     * The index in the playlist of the video that is currently playing. 
     */
    currentVideoIndex: number = 0;

    /**
     * The actual playlist of videos queued up to play. 
     */
    playlist: string[] = [];

    constructor(xmlFilePath: string)
    {
        // load the videos XML file
        var xmlhttp = new XMLHttpRequest();
        xmlhttp.open("GET", xmlFilePath, false);
        xmlhttp.send();
        var xmlDoc = xmlhttp.responseXML;

        // parse the XML list of videos
        for (var i = 0; i < xmlDoc.childNodes[0].childNodes.length; i++)
        {
            if (xmlDoc.childNodes[0].childNodes[i].nodeName == "Video")
            {
                var video = new Video();
                video.genre = xmlDoc.childNodes[0].childNodes[i].attributes.getNamedItem("genre").value;
                video.title = xmlDoc.childNodes[0].childNodes[i].attributes.getNamedItem("title").value;
                video.path = xmlDoc.childNodes[0].childNodes[i].attributes.getNamedItem("path").value;

                //console.log("Loaded video: " + video.path);

                // Add to all videos list
                this.videos.push(video);
            }
        }

        // sort all of the videos into alphabetical order
        this.videos.sort(CompareVideos);

        //for (var i = 0; i < this.videos.length; i++)
        //{
        //    var song = this.videos[i];

        //    var genre = this.genres[song.genre];
        //    if (genre) {
        //        // Only add the artist if it doesn't already exist
        //        if (genre.artists.indexOf(artist) == -1) {
        //            //console.log("Adding artist: " + artist.name);
        //            genre.artists.push(artist);
        //        }
        //    }
        //    else {
        //        //console.log("Creating genre: " + song.genre);
        //        //console.log("Adding artist: " + song.artist);

        //        genre = new Genre();
        //        genre.name = song.genre;
        //        genre.artists.push(artist);
        //        this.genres[song.genre] = genre;

        //        //this.genres[song.genre] = new Array();
        //        //this.genres[song.genre].push();
        //    }
        //}

        //this.ViewAllGenres(false);
        this.ViewAllVideos(false);
    }

    //ViewAllGenres(pushState: boolean) {
    //    if (pushState) {
    //        // push the state into the browser history so can navigate back to it
    //        // pushState(stateObject, title, url)
    //        history.pushState({ viewAllGenres: true }, "Unused", null);
    //    }

    //    currentFolder.innerHTML = "All Genres";

    //    thelist.innerHTML = "";

    //    for (var genreItem in this.genres) {
    //        // genreItem is just the NAME of the genre, which should be used as a key
    //        var listItem = this.genres[genreItem].CreateListItem();
    //        thelist.appendChild(listItem);
    //    }
    //}

    ViewAllVideos(pushState: boolean)
    {
        if (pushState)
        {
            // push the state into the browser history so can navigate back to it
            // pushState(stateObject, title, url)
            history.pushState({ viewAllSongs: true }, "Unused", null);
        }

        currentFolder.innerHTML = "All Songs";

        thelist.innerHTML = "";

        for (var videoItem in this.videos)
        {
            var listItem = this.videos[videoItem].CreateListItem();
            thelist.appendChild(listItem);
        }
    }

    GetNextVideo()
    {
        this.currentVideoIndex++;
        if (this.currentVideoIndex == this.playlist.length)
            this.currentVideoIndex = 0;

        return this.playlist[this.currentVideoIndex];
    }

    PlayNextVideo()
    {
        var path = this.GetNextVideo();
        var parts = path.split("/");
        videoTitle.innerHTML = parts[parts.length - 1];
        videoElement.src = path;
        videoElement.load();
    }

    //GetPrevVideo() {
    //    this.currentVideoIndex--;
    //    if (this.currentVideoIndex < 0)
    //        this.currentVideoIndex += this.playlist.length;

    //    return this.playlist[this.currentVideoIndex];
    //}

    // add the given Video node (and all other Videos at the same level) to the playlist [automatically starts playing the given Video node]
    AddToPlaylist(video: Video)
    {
        // clear the playlist
        this.playlist.length = 0;

        for (var i = 0; i < thelist.childNodes.length; i++)
        {
            //console.log(i);

            var videoPath: string = thelist.childNodes[i].attributes.getNamedItem("data-path").value;

            // if the current node is the one that was clicked, then mark it as the current video (-1 since getNextVideo auto increments by 1)
            if (videoPath == video.path)
            {
                this.currentVideoIndex = this.playlist.length - 1;
            }

            this.playlist.push(videoPath);
        }

        // automatically start playing the video that was clicked
        this.PlayNextVideo();
    }

    Search(query: string, pushState: boolean)
    {
        query = query.toLowerCase();

        if (pushState) {
            // push the state into the browser history so can navigate back to it
            // if the current state is a search, then replace it to prevent a stack of search history
            if (history.state != null && history.state.search) {
                //console.log("Replacing...");
                history.replaceState({ search: query }, "Unused", null);
            }
            else {
                history.pushState({ search: query }, "Unused", null);
            }
        }

        currentFolder.innerHTML = "Search: " + query;

        thelist.innerHTML = "";

        // find any related genres
        //for (var genreItem in this.genres) {
        //    if (genreItem.toLowerCase().indexOf(query) > -1) {
        //        thelist.appendChild(this.genres[genreItem].CreateListItem());
        //    }
        //}

        // find any related videos
        for (var videoItem in this.videos)
        {
            var video = this.videos[videoItem];
            if (video.title.toLowerCase().indexOf(query) > -1)
            {
                thelist.appendChild(video.CreateListItem());
            }
        }
    }
}

/**
 * Compares two Video objects to sort them.
 */
function CompareVideos(a: Video, b: Video)
{
    // first compare the genre
    //if (a.genre < b.genre)
    //    return -1;
    //if (a.genre > b.genre)
    //    return 1;

    // track name must be the same
    // so now compare the title
    if (a.title < b.title)
        return -1;
    if (a.title > b.title)
        return 1;

    // they must be the same song
    return 0;
}