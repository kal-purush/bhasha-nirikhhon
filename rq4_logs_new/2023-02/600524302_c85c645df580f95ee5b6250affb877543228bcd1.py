import os
import json
from pathlib import Path


class HtmlInfo:
    BASE_DIR = Path(__file__).resolve().parent

    def __init__(self):
        self.__data = self.__load_data(os.path.join(self.BASE_DIR, 'html_data.json'))

    @staticmethod
    def __load_data(path: str) -> dict:
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise Exception('Failed to load html_data.json file')

    def __valid_tag(self, tag: str) -> bool:
        my_filter = ['h' + str(i) for i in range(1, 7)]
        # Data cleaning
        temp_tag = tag[1:-1].lower()
        if temp_tag in my_filter:
            temp_tag = temp_tag[:-1] + str(1)
        temp_tag = '<' + temp_tag + '>'
        # Check if tag exist
        if temp_tag not in self.__data['tags'] and temp_tag not in self.__data['deprecated']:
            return False
        # Tag is valid return True
        return True

    def __valid_name(self, tag_name: str) -> bool:
        my_filter = ['h' + str(i) for i in range(1, 7)]
        # Data cleaning
        temp_tag = tag_name.lower()
        if temp_tag in my_filter:
            temp_tag = temp_tag[:-1] + str(1)
        temp_tag = '<' + temp_tag + '>'
        # Check if tag exist
        if temp_tag not in self.__data['tags'] and temp_tag not in self.__data['deprecated']:
            return False
        # Tag name is valid return True
        return True

    def tag_to_name(self, tag: str):
        """
        Convert tag to tag name <h1> -> h1
        """
        if not self.__valid_tag(tag):
            print(f'The tag you entered {tag} is invalid. please make sure you enter a correct tag.')
            return

        return tag[1:-1]

    def name_to_tag(self, tag_name):
        """
        Convert tag name to tag h1 -> <h1>
        """
        if not self.__valid_name(tag_name):
            print(f'The tag name you entered {tag_name} is invalid. please make sure you enter a correct tag name.')
            return

        return f'<{tag_name}>'

    def is_deprecated(self, tag: str):
        """
        Returns True if the provided tag is deprecated
        """
        if not self.__valid_tag(tag):
            print(f'The tag you entered {tag} is invalid. please make sure you enter a correct tag.')
            return

        return tag in self.__data['deprecated']

    def is_html_tag(self, word: str) -> bool:
        """
        Check if a html tag exist using the tag or the tag name
        """
        return self.__valid_tag(word) or self.__valid_name(word)

    def all_tags(self, deprecated=False):
        """
        Returns all tags if deprecated is set to True
        """
        tags_list = list(self.__data['tags'].keys())
        if deprecated:
            tags_list += list(self.__data['deprecated'].keys())

        return tags_list

    def tag_description(self, tag: str):
        """
        Returns the tag description if it exists
        """
        tag_key = None
        if self.__valid_tag(tag):
            tag_key = tag
        elif self.__valid_name(tag):
            tag_key = f'<{tag}>'
        else:
            print(f'The tag you entered {tag} is invalid. please make sure you enter a correct tag.')
            return

        tag_description = self.__data['tags'].get(tag_key, None)
        if not tag_description:
            tag_description = self.__data['deprecated'].get(tag_key)
            tag_description += ' (This tag is deprecated)'

        return tag_description