

class Media {
    constructor(mediaType, itemId, title, author, year, finished, review, notes){
        this.mediaType = mediaType;
        this.itemId = itemId;
        this.title = title;
        this.author = author;
        this.year = year;
        this.finished = finished;
        this.review = review;
        this.notes = notes;
    }

    info(){
        const bookInfo = `${this.title} by ${this.author}. ${this.pages} pages. Read? ${this.consumedStatus}`;
        return bookInfo
    }
}


function Media2 (mediaType, itemId, title, author, year, finished, review, notes){
    this.mediaType = mediaType;
    this.itemId = itemId;
    this.title = title;
    this.author = author;
    this.year = year;
    this.finished = finished;
    this.review = review;
    this.notes = notes;
    this.info = function() {
        const bookInfo = `${this.title} by ${this.author}. ${this.pages} pages. Read? ${this.consumedStatus}`;
        return bookInfo
    }
  }