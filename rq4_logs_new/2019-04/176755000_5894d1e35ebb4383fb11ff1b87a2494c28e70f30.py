import re
import webbrowser

import click
import yaml
from yaml.loader import Loader

from yeahyeah.core import YeahYeahMenuItem, YeahYeahPlugin


class UrlPattern(YeahYeahMenuItem):
    """A named url pattern that launches some url and can be saved to disk"""

    def __init__(self, name, pattern, help_text=None):
        super().__init__(name, help_text)

        self.pattern = pattern

    def __str__(self):
        return f"URLPattern {self.name}:{self.pattern}"

    @property
    def help_text(self):
        if self._help_text is None:
            return f"launch {self.name}"
        else:
            return self._help_text

    def render(self, args_tuple):
        """Render this url pattern with the given args

        Parameters
        ----------
        args_tuple: Tuple
            tuple of all arguments

        Returns
        -------
        str

        """
        return self.pattern.format(*args_tuple)

    def to_click_command(self):
        """This url pattern as a click command that can be added with add_command()
        """

        arguments = re.findall(r"\{([^{}]*)\}", self.pattern)

        @click.command(name=self.name, help=self.help_text)
        def the_command(**kwargs):
            url = self.pattern.format(**kwargs)
            click.echo(url)
            open_url(url)

        for argument_name in arguments:
            the_command = click.argument(argument_name, type=click.STRING)(the_command)

        return the_command

    @classmethod
    def to_yaml(cls, dumper, obj):
        """Dump to yaml as dictionary"""
        return dumper.represent_dict(obj.to_dict())

    def to_dict(self):
        """Represent this UrlPattern as a dictionary:

        """
        values = {"pattern": self.pattern}
        if self._help_text is not None:
            values["text"] = self._help_text
        return {self.name: values}


class WildCardUrlPattern(UrlPattern):
    def __init__(self, name, pattern, help_text=None):
        super().__init__(name, pattern, help_text)

    def to_click_command(self):
        """This url pattern as a click command that can be added with add_command()
        """

        arguments = re.findall(r"\{([^{}]*)\}", self.pattern)

        @click.command(name=self.name, help=self.help_text)
        def the_command(**kwargs):

            param_name, param_values = list(kwargs.items()).pop()
            url = self.pattern.format(**{param_name: " ".join(param_values)})
            click.echo(f"loading {url}")
            open_url(url)

        for argument_name in arguments:
            the_command = click.argument(argument_name, type=click.STRING, nargs=-1)(
                the_command
            )

        return the_command

    def to_dict(self):
        """Represent this UrlPattern as a dictionary:

        """
        initial = super().to_dict()
        initial[self.name]["capture_all_keywords"] = True
        return initial


class URLPatternFactory:
    """Can load UrlPattern of different types from dict"""

    @staticmethod
    def from_dict(dict_in):
        """Create a UrlPattern object from given dict.

        Dict should have been created with to_dict()
        """
        name = list(dict_in.keys()).pop()
        values = dict_in[name]
        pattern = values["pattern"]
        help_text = values.get("text", None)
        if "capture_all_keywords" in values.keys():
            return WildCardUrlPattern(name=name, pattern=pattern, help_text=help_text)
        else:
            return UrlPattern(name=name, pattern=pattern, help_text=help_text)


class URLPatternList:
    """A persistable list of url path_items"""

    def __init__(self, patterns):
        """

        Parameters
        ----------
        patterns: List[UrlPattern]
            list of path_items in this list
        """
        self.patterns = patterns

    def save(self, file):
        """Save list to file

        Parameters
        ----------
        file: Open file handle
            save to this file

        Returns
        -------

        """
        yaml.dump(self.to_dict(), file, default_flow_style=False)

    def append(self, item):
        self.patterns.append(item)

    def __iter__(self):
        return self.patterns.__iter__()

    def __len__(self):
        return self.patterns.__len__()

    def remove(self, item):
        return self.patterns.remove(item)

    def to_dict(self):
        """This URLPatternList as dict, as terse as possible:

        {pattern_name1: {param1: value1,...},
         pattern_name2: {param2: value2,...},
        """
        result = {}
        for pattern in self.patterns:
            pattern_dict = pattern.to_dict()
            result.update(pattern_dict)

        return result

    @staticmethod
    def load(file):
        """Try to load a UrlPatternList from file handle

        Parameters
        ----------
        file: open file hanle

        Returns
        -------
        URLPatternList

        Raises
        ------
        TypeError:
            When object loaded is not a list


        """
        loaded = yaml.load(file, Loader=Loader)

        if type(loaded) is not dict:
            msg = f"Expected to load a dictionary, but found {type(loaded)} instead"
            raise TypeError(msg)
        # flatten to list of dicts
        pattern_list = []
        for key, values in loaded.items():
            pattern_list.append(URLPatternFactory.from_dict({key: values}))

        return URLPatternList(patterns=pattern_list)


def open_url(url):
    """Open a browser with the given url

    Parameters
    ----------
    url: str

    Returns
    -------
    None

    """
    webbrowser.open_new(url)


class UrlPatternsPlugin(YeahYeahPlugin):
    def __init__(self, pattern_list):
        """Plugin that holds URL path_items

        Parameters
        ----------
        pattern_list: URLPatternList

        """
        super().__init__(slug="url_patterns", short_slug='url')
        self.pattern_list = pattern_list
        self.config_file_path = None

    @classmethod
    def __from_file_path__(cls, config_file_path):
        cls.assert_config_file(config_file_path)
        with open(config_file_path, "r") as f:
            pattern_list = URLPatternList.load(f)

        obj = cls(pattern_list=pattern_list)
        obj.config_file_path = config_file_path
        return obj

    def save(self):
        """Save current pattern list to disk if possible"""
        if self.config_file_path:
            with open(self.config_file_path, 'w') as f:
                self.pattern_list.save(file=f)

    @staticmethod
    def assert_config_file(config_file_path):
        """Make sure config file exists. If not, create an example config file"""
        if config_file_path.exists():
            return
        else:
            config_file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_file_path, "w") as f:
                example_patterns = [
                    UrlPattern(
                        name="virus",
                        pattern="https://www.virustotal.com",
                        help_text="Launch online virus scanner",
                    ),
                    UrlPattern(
                        name="wiki",
                        pattern="https://en.wikipedia.org/wiki/{article_slug}",
                        help_text="Launch given mediawiki article in English",
                    ),
                    WildCardUrlPattern(
                        name="search", pattern="https://duckduckgo.com/?q={query}"
                    ),
                ]
                URLPatternList(patterns=example_patterns).save(f)
            click.echo(
                f"UrlPattern config file {config_file_path} did not exist. Creating with default contents.."
            )

    def get_menu_items(self):
        """

        Returns
        -------
        List[UrlPattern]
        """
        return self.pattern_list.patterns

    def get_admin_commands(self):
        """

        Returns
        -------
        List[click.Command]
            list of click commands that can be used to admin this plugin

        """

        @click.command()
        def status():
            """Print some info for this plugin"""
            status_str = f"UrlPatternsPlugin:\n" \
                         f"{len(self.get_menu_items())} path_items in plugin\n"
            if self.config_file_path:
                status_str += f"Config file: {self.config_file_path}"
            click.echo(status_str)

        @click.command()
        @click.argument('keyword')
        @click.argument('pattern')
        def add(keyword, pattern):
            """Add a new url pattern"""
            pattern = UrlPattern(name=keyword, pattern=pattern)
            click.echo(f"Adding {pattern}")
            self.pattern_list.append(pattern)
            self.save()

        @click.command()
        @click.argument('keyword')
        def remove(keyword):
            """Remove an existing url pattern"""
            to_remove = [x for x in self.pattern_list if x.name == keyword]
            if not to_remove:
                click.echo(f"Pattern with keyword {keyword} not found")
            else:
                click.echo(f"Removing {[str(x) for x in to_remove]}")
                for x in to_remove:
                    self.pattern_list.remove(x)
                self.save()

        @click.command()
        def list():
            """list all url path_items"""
            click.echo("\n".join([str(x) for x in self.pattern_list]))

        return [status, list, add, remove]