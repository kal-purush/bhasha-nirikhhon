import { Response, Request } from 'express'
import Book from '../models/Book';
import User from '../models/User';
import puppeteer from 'puppeteer';

const scrapeArticles = async (title: string, author: string) => {
    const browser: puppeteer.Browser = await puppeteer.launch({ headless: false });

    const page: puppeteer.Page = await browser.newPage();
    await page.goto("https://www.nytimes.com/");

    const inputText = title + " " + author;

    await page.click("button.er09x8g1");
    await page.type("input.css-1axrnfw", inputText);
    await page.click("button.css-1gudca6");
    await page.waitForXPath('//div[@class="css-nhmgdh"]');

    const element1 = await page.waitForSelector("h4.css-2fgx4k");
    const value1 = await element1!.evaluate((el) => el.textContent);

    const element2 = await page.waitForSelector("p.css-16nhkrn");
    const value2 = await element2!.evaluate((el) => el.textContent);
    await browser.close();

    return {
        title: value1,
        content: value2,
    };
};

export const getLivePosts = async(req: Request, res: Response): Promise<Response> => {
    try{

        let articles: any[] = [];
        const result = await User.findOne({email: req.params.email});

        if(result){
           articles = await Promise.all(Object.values(result)[Object.values(result).length - 2].favorites.map( async (id: string) => {
                const book = await Book.findById(id);
                if(book){
                const article = await scrapeArticles(Object.values(book)[Object.values(book).length - 2].title,Object.values(book)[Object.values(book).length - 2].author);
                    return article;
                }
            }))
        }
        
        return res.status(200).json({results: articles});
    }catch(err){

        return res.status(404).json(err);
    }
}