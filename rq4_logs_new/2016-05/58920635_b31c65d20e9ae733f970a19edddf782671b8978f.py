# This example show how to write an inline mode telegramt bot use pyTelegramBotAPI.
import telebot
import time
import sys
import logging
import urllib2
import re
from telebot import types
from HTMLParser import HTMLParser

API_TOKEN = '227539123:AAFr3YFdV9OElqBIOtB5lW5FH1ERGPBvgvA'

bot = telebot.TeleBot(API_TOKEN)
telebot.logger.setLevel(logging.DEBUG)
niv_thumb = '~/Desktop/Telebots-master/pyTelegramBotAPI/examples/niv.jpeg'
kjv_thumb = '~/Desktop/Telebots-master/pyTelegramBotAPI/examples/kjv.jpg'



class MyHTMLParser(HTMLParser):

    triggerTitle = False
    triggerPassage = False

    titleData = ''
    passageData = ''''''

    def handle_starttag(self, tag, attrs):
        if tag == 'span' and attrs == [('class', 'passage-display-bcv')]:
          self.triggerTitle = True

        if tag == 'span' and (len(attrs) > 1):

          if ("en-NIV-" in attrs[0][1]):
            self.triggerPassage = True

    def handle_endtag(self, tag):
        if tag == 'span':
          self.triggerPassage = False


    def handle_data(self, data):
      if self.triggerTitle:
        self.titleData = data
        self.triggerTitle = False


      if self.triggerPassage:
        self.passageData += data

        
# Splits the query into book, chapter(optional) and verseTo(optional) and verseFrom(optional) and returns them. Returns a None if it isn't specified
# Input is a string. Pass query.query into input to use the function
# Needs to handle books like 1 Kings, 2Kings
# Needs to handle a range of verses e.g. John 3:16-22
# Needs error handling if range of verses exceed actual number of verses in the book
def query_splitter(query_string):
  query_string = "".join(query_string.split()) #this removes all whitespace from the query
  regex = r"([0-9]?[a-zA-Z]+)([0-9]+)(:[0-9]+)?(-[0-9]+)?"
  match = re.search(regex,query_string)
  print match
  book = match.group(1)
  print book
  chapter = match.group(2)
  print chapter
  verseFrom = match.group(3)
  if verseFrom != None:
    verseFrom = verseFrom[1:]
  print verseFrom
  verseTo = match.group(4)
  if verseTo != None:
    verseTo = verseTo[1:]
  print verseTo
  return book,chapter,verseFrom,verseTo



@bot.inline_handler(lambda query: len(query.query) > 0)
def query_text(inline_query):
    try:
        kjv = open ('kjv.txt')
        passageResult = ""
        titleResult = ""
        parser = MyHTMLParser()

        print '\n\n\n****'
        print inline_query.query
        print '********\n\n\n'

        text = inline_query.query
        text = text.replace(' ', '+')
        text = text.replace(':','%3A')


        urlNameNIV = 'https://www.biblegateway.com/passage/?search=' + text + '&version=NIV'
        pageNIV = urllib2.urlopen(urlNameNIV).read()
        parser.feed(pageNIV)

        bookQ,chapterQ,verseFromQ,verseToQ  = query_splitter(inline_query.query) 

        for line in kjv:
          book, remaining = line.split(" ",1)
          chapter, verse = remaining.split(":", 1)
          verse, passage = verse.split(" ",1)
          if bookQ.lower() == book.lower() and chapterQ.lower() == chapter.lower(): #book and chapter match
            if verseFromQ == verse and verseToQ == None:                            #verse match
              passageResult += passage
              titleResult = book + " " + chapter + ":" + verseFromQ
            elif verseFromQ == None:                                                #verse not specified
              passageResult += passage
              titleResult = book + " " + chapter


          else:
            pass








              
          

        kjv.close()



        r = types.InlineQueryResultArticle('1', parser.titleData + " (NIV)", types.InputTextMessageContent(parser.titleData + " (NIV)" + '\n' + parser.passageData), description = parser.passageData)
        r2 = types.InlineQueryResultArticle('2', titleResult + " (KJV)", types.InputTextMessageContent(titleResult + " (KJV)"'\n' + passageResult), description = passageResult)
        bot.answer_inline_query(inline_query.id, [r,r2])
    except Exception as e:
        print (e)


# @bot.inline_handler(lambda query: query.query == 'photo1')
# def query_photo(inline_query):
#     try:
#         r = types.InlineQueryResultPhoto('1',
#                                          'https://raw.githubusercontent.com/eternnoir/pyTelegramBotAPI/master/examples/detailed_example/kitten.jpg',
#                                          'https://raw.githubusercontent.com/eternnoir/pyTelegramBotAPI/master/examples/detailed_example/kitten.jpg',
#                                          input_message_content=types.InputTextMessageContent('hi'))
#         r2 = types.InlineQueryResultPhoto('2',
#                                           'https://raw.githubusercontent.com/eternnoir/pyTelegramBotAPI/master/examples/detailed_example/rooster.jpg',
#                                           'https://raw.githubusercontent.com/eternnoir/pyTelegramBotAPI/master/examples/detailed_example/rooster.jpg')
#         bot.answer_inline_query(inline_query.id, [r, r2], cache_time=1)
#     except Exception as e:
#         print(e)


# @bot.inline_handler(lambda query: query.query == 'video')
# def query_video(inline_query):
#     try:
#         r = types.InlineQueryResultVideo('1',
#                                          'https://github.com/eternnoir/pyTelegramBotAPI/blob/master/tests/test_data/test_video.mp4?raw=true',
#                                          'video/mp4', 'Video',
#                                          'https://raw.githubusercontent.com/eternnoir/pyTelegramBotAPI/master/examples/detailed_example/rooster.jpg',
#                                          'Title'
#                                          )
#         bot.answer_inline_query(inline_query.id, [r])
#     except Exception as e:
#         print(e)


# @bot.inline_handler(lambda query: len(query.query) is 0)
# def default_query(inline_query):
#     try:
#         r = types.InlineQueryResultArticle('1', 'default', 'default')
#         bot.answer_inline_query(inline_query.id, [r])
#     except Exception as e:
#         print(e)


def main_loop():
    bot.polling(True)
    while 1:
        time.sleep(3)


if __name__ == '__main__':
    try:
        main_loop()
    except KeyboardInterrupt:
        print >> sys.stderr, '\nExiting by user request.\n'
        sys.exit(0)