import * as fs from 'fs';
import { promisify } from 'util';
import { buffer2book } from './epub';
import { Book } from './model';

const staticLocation = 'public/';
const cacheLocation = '';
export async function openBook(bookName: string): Promise<Book | undefined> {
    // Try to read from cache
    if (await fileExists(jsonPath(bookName))) {
        const jsonFile = await fileOpen(jsonPath(bookName), 'utf8');
        const json = JSON.parse(jsonFile);
        return json as Book;
    }

    if (await fileExists(epubPath(bookName))) { // Check if epub file exist
        const epubFile = await fileOpen(epubPath(bookName));
        const book = buffer2book(epubFile);
        const bookString = JSON.stringify(book);
        fileWrite(jsonPath(bookName), bookString);

        return book;
    }

    return undefined;
}

function epubPath(bookName: string) {
    return staticLocation + bookName + '.epub';
}

function jsonPath(bookName: string) {
    return staticLocation + cacheLocation + bookName + '.json';
}

const fileExists = promisify(fs.exists);
const fileOpen = promisify(fs.readFile);
const fileWrite = promisify(fs.writeFile);