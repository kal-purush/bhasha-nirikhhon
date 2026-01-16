import axios from "axios";

const getMovieByTitle = async (title, type, year, page) => {
    let call = `http://www.omdbapi.com/?apikey=7f7ea64d&s=${title}`;
    if (type) call = call + "&type=" + type;
    if (year) call = call + "&y=" + year;
    if (page) call = call + "&page=" + page;
    return await axios.get(call);
}

export default getMovieByTitle;