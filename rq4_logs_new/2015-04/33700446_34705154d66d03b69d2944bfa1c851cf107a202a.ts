class Genre
{
    /*
     * The name of this genre.
     */
    name: string = "Unknown";

    /*
     * The list of movies associated with this genre.
     */
    movies: Video[] = new Array();

    /*
     * The list of TV shows associated with this genre.
     */
    shows: TvShow[] = new Array();

    CreateListItem()
    {
        var listItem = document.createElement("li");

        listItem.innerHTML = "<div><img src=\"icons/genre.png\"/> " + this.name + " (" + this.movies.length + " movies & " + this.shows.length + " shows)</div>";

        var onclick = function () { this.Open(true); };

        // we must bind the click method to this Genre object in order to make the "this" object in the handler point to this Genre object
        listItem.onclick = onclick.bind(this);

        return listItem;
    }

    Open(pushState: boolean)
    {
        if (pushState)
        {
            // push the state into the browser history so can navigate back to it
            // pushState(stateObject, title, url)
            history.pushState({ genre: this.name }, "", null);
        }
        currentFolder.innerHTML = this.name;

        thelist.innerHTML = "";

        for (var i = 0; i < this.shows.length; i++)
        {
            var show = this.shows[i];
            var listItem = show.CreateListItem();
            thelist.appendChild(listItem);
        }

        for (var j = 0; j < this.movies.length; j++)
        {
            var movie = this.movies[j];
            var movieListItem = movie.CreateListItem();
            thelist.appendChild(movieListItem);
        }
    }
} 