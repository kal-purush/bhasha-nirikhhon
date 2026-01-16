import Artist from '@/utils/artist.js';
import slug   from 'slug';

export default class BlogPost {

    /**
     * @param {string} title
     * @param {string} metaTitle
     * @param {string} metaDescription
     * @param {string} body
     * @param {string} imageUrl
     * @param {string} authorUUID
     */
    constructor(title, metaTitle, metaDescription, body, imageUrl, authorUUID) {

        Artist.isString(title).throwIfFalse('\'BlogPost.<init>.title\' isn\'t a string');
        Artist.isString(metaTitle).throwIfFalse('\'BlogPost.<init>.metaTitle\' isn\'t a string');
        Artist.isString(metaDescription).throwIfFalse('\'BlogPost.<init>.metaDescription\' isn\'t a string');
        Artist.isString(body).throwIfFalse('\'BlogPost.<init>.body\' isn\'t a string');
        Artist.isString(imageUrl).throwIfFalse('\'BlogPost.<init>.imageUrl\' isn\'t a string');
        Artist.isString(authorUUID).throwIfFalse('\'BlogPost.<init>.authorUUID\' isn\'t a string');

        this.title    = title;
        this.meta     = {
            title      : metaTitle,
            description: metaDescription,
            authorUUID : authorUUID,
            slug       : slug(title),
        };
        this.body     = body;
        this.imageUrl = imageUrl;
    }
}