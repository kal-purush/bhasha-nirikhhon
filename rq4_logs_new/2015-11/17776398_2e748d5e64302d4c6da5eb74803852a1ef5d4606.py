#!/usr/bin/python2 -tt
# -*- coding: utf-8 -*-

# Import {{{
import re
from operator import itemgetter
from filecache import filecache
from multiprocessing.dummy import Pool, cpu_count, Lock
from optparse import OptionParser
from lib.common import (
    get_file_path,
    dump_json_file,
    prepare_opener,
    open_url,
    get_parsed_url_response,
    print_progress,
    print_progress_end
)
# }}}

LOCK = Lock()

def map_category_name(category):
    category_name_to_url = {
        'film':  'movie-review',
        'tv':    'tv-club',
        'books': 'book-review',
        'music': 'music-review',
    }

    if category_name_to_url.has_key(category):
        category = category_name_to_url[category]

    return category

def prepare_av_opener(av_url='http://www.avclub.com/'):
    opener = prepare_opener(av_url)

    # Request used to initialize cookie.
    open_url(av_url, opener)

    return opener

def get_parsed_av_url_response(url, opener=prepare_av_opener()):
    return get_parsed_url_response(url, opener=opener)

# Invalidate values after 30 days.
@filecache(30 * 24 * 60 * 60)
def scrap_article_info(href):
    article_url = 'http://www.avclub.com{0}'.format(href)
    response    = get_parsed_av_url_response(article_url)
    if not response: return

    rating_panel = response.find('div', { 'class': re.compile('rating panel') })
    title_tag    = rating_panel.find('h2', { 'class': 'title' })
    grade_tag    = rating_panel.find('div', { 'class': re.compile('grade letter') })
    heading_tag  = response.find('div', { 'class': re.compile('article-header') }).find(
        'h1', { 'class': 'heading' }
    )
    director_tag = rating_panel.find('div', { 'class': 'director' })

    article_info = {
        'title':    title_tag.text.strip() if title_tag else None,
        'grade':    grade_tag.text if grade_tag else None,
        'heading':  heading_tag.text if heading_tag else None,
        'director': director_tag.contents[-1].strip() if director_tag else None,
        'href':     article_url,
    }

    response.decompose()

    return article_info

def scrap_article_entry(article):
    heading      = article.find('h1', { 'class': 'heading' }).find('a')
    article_info = scrap_article_info(heading['href']) if heading else None

    with LOCK: print_progress()

    return article_info

def scrap_category_reviews(input_category, last_page):
    category     = map_category_name(input_category)
    category_url = 'http://www.avclub.com/features/{0}/?page='.format(category)
    articles_re  = re.compile('article-body')
    grade_re     = re.compile('grade')

    # Review grade can be in range <A, B->
    grade_val_re = re.compile('^(A|B)')

    # Create workers pool.
    workers_count = cpu_count() * 2
    pool          = Pool(workers_count)

    page_index, scraped_reviews = 1, []
    while True:
        # Fetch next page with reviews.
        index_url   = '{0}{1}'.format(category_url, page_index)
        response    = get_parsed_av_url_response(index_url)

        # Next page with reviews not found - there are no more reviews available.
        if not response: break

        # Find all articles on page.
        wrapper  = response.find('div', { 'class': articles_re })
        articles = wrapper.findAll('article') if wrapper else None

        # Process found articles.
        if articles:
            # Filter articles by review grade.
            filtered_articles = filter(
                lambda a: grade_val_re.match(
                    a.find('div', { 'class': grade_re }).text
                ),
                articles
            )
            if filtered_articles:
                # Fetch filtered articles info.
                page_reviews = pool.map(scrap_article_entry, filtered_articles)
                # Append found reviews.
                scraped_reviews += page_reviews

        # Save some memory.
        response.decompose()

        # Go to next page.
        page_index += 1

        # Limit reviews scraping to given first pages.
        if last_page and page_index == last_page: break

    # Sort reviews by grade.
    scraped_reviews.sort(key=itemgetter('grade'))
    print_progress()

    print_progress_end()

    return scraped_reviews

def main():
    # Cmd options parser
    option_parser = OptionParser()

    # Add options
    category_choices = ['film', 'tv', 'music', 'books']
    option_parser.add_option(
        "-c",
        "--category",
        type='choice',
        choices=category_choices,
        help="Choose one of {0}".format("|".join(category_choices)),
    )
    option_parser.add_option(
        "-p",
        "--page",
        type='int',
        help="Limit reviews scraping to given first pages",
    )

    (options, args) = option_parser.parse_args()

    if not options.category:
        option_parser.print_help()
    else:
        print(u'Fetching reviews from category "{0}"'.format(options.category))
        scraped_reviews = scrap_category_reviews(
            options.category,
            options.page,
        )
        if scraped_reviews:
            file_name = 'avclub_{0}.json'.format(options.category)
            dump_json_file(scraped_reviews, get_file_path(file_name))

if __name__ == "__main__":
    main()