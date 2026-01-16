import re

COMMA_MATCHER = re.compile(r',\s+')


class BlogPost:

    def __init__(self, page):
        self.page = page

    @property
    def title(self):
        return self.page.meta.get('title')

    @property
    def date(self):
        return self.page.meta.get('date')

    @property
    def is_published(self):
        return self.page.meta.get('published')

    @property
    def tags(self):
        if self.page.meta.get('tags') is None:
            return set()
        else:
            tags = COMMA_MATCHER.sub(',', self.page.meta.get('tags'))
            return {t.strip().lower() for t in tags.split(',')}

    def from_year(self, year):
        return self.date.year == year

    def has_tag(self, tag):
        return tag in self.tags