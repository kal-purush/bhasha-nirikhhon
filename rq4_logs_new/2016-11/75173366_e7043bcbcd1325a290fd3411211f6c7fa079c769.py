def ogParser(metatag):

        for i in soup.find_all("meta"):
            try:
                # count=0
                if i.get("property", None) == metatag.lower():
                    print (metatag,i.get("content", None))
                    og.update({metatag:i.get("content", None).encode('utf-8')})
                    break
                elif i.get("itemprop", None).lower() == metatag.lower():
                    print (metatag, i.get("content", None))
                    og.update({metatag: i.get("content", None).encode('utf-8')})
                    break
                elif i.get("name", None).lower() == metatag.lower():
                    print (metatag, i.get("content", None))
                    og.update({metatag: i.get("content", None).encode('utf-8')})
                    break
            except AttributeError:
                pass
        else:
            og.update({metatag:None})



def ogParserFilter():
    og.update({"category":catline.rstrip('\n')})
    og.update({"urlLink":line.rstrip('\n')})
    #itemprop
    ogParser("dateModified")
    ogParser("datePublished")
    ogParser("articleSection")
    ogParser("image")

    #property
    ogParser("og:locale")
    ogParser("article:published_time")
    ogParser("article:author")

    ogParser("og:site_name") #
    ogParser("og:title") #
    ogParser("og:url") #
    ogParser("og:type") #
    ogParser("og:description")
    ogParser("og:image")
    ogParser("og:descdsdsdsdion")

    # Information about article
    ogParser("geo") # Location of the Article
    ogParser("per") # Person inside the Article
    ogParser("article:publisher")
    ogParser("og:article:published_time")
    ogParser("og:article:modified_time")
    ogParser("article:tag")
    ogParser("og:pubdate")
    ogParser("pubdate")
    ogParser("lastmod")
    ogParser("section") # The section of the newspaper that reference it
    ogParser("Subsection") # WIRED The Subsection of the newspaper that reference it
    ogParser("article:section") # The section of the newspaper that reference it
    ogParser("article:section_url") # The section of the newspaper that reference it
    ogParser("article:published") # DatePublished
    ogParser("article:modified") # DatePublished
    ogParser("author") # The author of the article


    # Normal Meta
    ogParser("description") #
    ogParser("title") #
    ogParser("keywords") #
    ogParser("news_keywords") #

    # Facebook data
    ogParser("fb:app_id") #
    ogParser("fb:pages") #

    # Twitter OG
    ogParser("twitter:card") #
    ogParser("twitter:description") #
    ogParser("twitter:site") #
    ogParser("twitter:title") #
    ogParser("twitter:url") #
    ogParser("twitter:creator") #