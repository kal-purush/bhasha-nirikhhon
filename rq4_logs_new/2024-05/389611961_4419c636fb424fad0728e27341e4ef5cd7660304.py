from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand
from django.db import transaction, models
from wagtail.models import Page


class Command(BaseCommand):
    help = "Move citation from endnotes to dedicated citation field"

    @transaction.atomic
    def handle(self, *args, **options):
        for page in Page.objects.filter(locale__language_code="en").specific().all():
            if not hasattr(page, "endnotes"):
                continue
            endnotes = page.endnotes
            if "DOI" not in endnotes:
                continue
            html_content = BeautifulSoup(endnotes, features="lxml")
            ps = html_content.find_all("p")
            block_to_remove = None
            for p in ps:
                text = p.get_text()
                if "DOI" not in text:
                    continue
                text = text.replace(
                    "Smart Forests Atlas materials are free to use for non-commercial purposes "
                    "(with attribution) under a CC BY-NC-SA 4.0 license. To cite this radio episode: ",
                    "",
                )
                page.citation = text
                block_to_remove = p.attrs.get("data-block-key")
                p.extract()
                new_content = (
                    str(html_content)
                    .replace("<html>", "")
                    .replace("<body>", "")
                    .replace("</html>", "")
                    .replace("</body>", "")
                )
                page.endnotes = new_content
            print(f"Updated page: {page}")
            page.save()

            if page.citation and block_to_remove:
                translated_pages = Page.objects.filter(
                    ~models.Q(locale__language_code="en")
                    & models.Q(translation_key=page.translation_key)
                ).specific()
                for translated_page in translated_pages:
                    html_content = BeautifulSoup(
                        translated_page.endnotes, features="lxml"
                    )
                    ps = html_content.find_all("p")
                    for p in ps:
                        if p.attrs.get("data-block-key") == block_to_remove:
                            p.extract()
                    new_content = (
                        str(html_content)
                        .replace("<html>", "")
                        .replace("<body>", "")
                        .replace("</html>", "")
                        .replace("</body>", "")
                    )
                    translated_page.endnotes = new_content
                    translated_page.citation = page.citation
                    translated_page.save()
                    print(f"Updated translated page: {page}")