from django.utils.feedgenerator import Rss201rev2Feed

class CustomFeedGenerator(Rss201rev2Feed):
    def add_item_elements(self, handler, item):
        super(CustomFeedGenerator, self).add_item_elements(handler, item)
        handler.addQuickElement(u"carousel", item['carousel'])
        handler.addQuickElement(u"thumbnail", item['thumbnail'])