import sliceString from "../../../utils/sliceString";
import isWordInString from "../../../utils/isWordInString";

import {IArticle} from "../../../models/article";

import {environment} from "../../../environment";

const TITLE_FIND_WEIGHT = 1;
const SUMMARY_FIND_WEIGHT = 0.6;


export default async function setArticlesPriority(articles: IArticle[], filter: string): Promise<IArticle[]> {
    const words = [...new Set(filter.toLowerCase().split(" "))];

    return articles.map((article) => {
        let priority = 0;

        words.forEach((word) => {
            const slicedTitle = sliceString(article.title, environment.MAX_ARTICLE_TEXT_LENGTH);
            const slicedSummary = sliceString(article.summary, environment.MAX_ARTICLE_TEXT_LENGTH);

            if( isWordInString(word,slicedTitle )) priority += TITLE_FIND_WEIGHT;
            if( isWordInString(word, slicedSummary )) priority += SUMMARY_FIND_WEIGHT;
        });

        return {...article, priority};
    }, []);

}