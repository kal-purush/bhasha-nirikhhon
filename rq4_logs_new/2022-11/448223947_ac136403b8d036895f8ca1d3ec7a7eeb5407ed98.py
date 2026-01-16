import collections
import json
import logging
import operator
import os

from datetime import datetime
from typing import List
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import humanize

from jinja2 import Environment, FileSystemLoader, select_autoescape
from prettytable import PrettyTable

from .launchpadtools import TypeLPObject


__THIS__ = os.path.dirname(os.path.abspath(__file__))
NOW = datetime.now(tz=ZoneInfo("UTC"))


class BaseBuildsReport:
    """Base class report"""
    def __init__(self, output: str = None):
        """Initialize base report."""
        self.output = output
        self.log = logging.getLogger(f'{__name__}.{self.__class__.__name__}')

    def add_build(self, charm_project, recipe, build: TypeLPObject):
        """Add a new build object to the report.

        :param charm_project: charm project the build belongs to.
        :param build: build to add to the report.
        """
        raise NotImplementedError()

    def generate(self):
        """Generate report."""
        raise NotImplementedError()


class HtmlBuildsReport(BaseBuildsReport):
    def __init__(self, output):
        super().__init__(output)
        # {'openstack': {'yoga': {'edge': [<build>, <build>,...]}}}
        self.builds = collections.defaultdict(
            lambda: collections.defaultdict(
                lambda: collections.defaultdict(
                    list
                )
            )
        )

    def add_build(self, charm_project, recipe, build):
        # the same build could have been used to be released into multiple
        # channels.
        for channel in recipe.store_channels:
            (track, risk) = channel.split('/')
            self.builds[charm_project.project_group][track][risk].append(
                (charm_project, recipe, build)
            )

    def generate(self):
        reports_written = collections.defaultdict(
            lambda: collections.defaultdict(dict)
        )
        os.makedirs(self.output, exist_ok=True)
        env = self._get_jinja2_env()
        template = env.get_template('all_builds.html.j2')
        for project, tracks in self.builds.items():
            for track, risks in tracks.items():
                for risk, builds in risks.items():
                    channel = f'{track}/{risk}'
                    content = template.stream({'all_builds': builds,
                                               'project': project,
                                               'track': track,
                                               'risk': risk,
                                               'channel': channel,
                                               'NOW': NOW})

                    fname = f'{project}-{track}-{risk}.html'
                    report_file = os.path.join(self.output, fname)
                    self.log.info('writing html report to %s for channel %s',
                                  report_file, channel)
                    with open(report_file, 'w') as f:
                        content.dump(f)
                        f.write('\n')

                    reports_written[project][track][risk] = fname

        # generate index.
        template = env.get_template('index.html.j2')
        content = template.stream({'reports': reports_written,
                                   'NOW': NOW})

        with open(os.path.join(self.output, 'index.html'), 'w') as f:
            self.log.info('Writing html report index to %s', f.name)
            content.dump(f)
            f.write('\n')

    def _get_jinja2_env(self):
        return Environment(
            loader=FileSystemLoader([os.path.join(__THIS__, 'templates')]),
            extensions=["jinja2_humanize_extension.HumanizeExtension"],
            autoescape=select_autoescape()
        )


class PlainBuildsReport(BaseBuildsReport):
    def __init__(self, output: str = None):
        """Initialize plain builds report."""
        super().__init__(output)
        self.t = PrettyTable()
        self.cols = ['Recipe Name', 'Channels', 'Arch', 'State', 'Age',
                     'Revision', 'Store Rev', 'Build Log']
        self.t.field_names = self.cols
        self.t.align = 'l'  # align to the left.

    def add_build(self, charm_project, recipe, build):
        build_arch_tag = build.distro_arch_series.architecture_tag
        series_arch = f'{build.distro_series.name}/{build_arch_tag}'
        age = humanize.naturaltime(build.datebuilt, when=NOW)
        if build.buildstate != 'Successfully built':
            build_log = build.build_log_url
        else:
            build_log = ''

        try:
            # git commit hash short version
            revision = build.revision[:7]
        except Exception:
            self.log.debug('Cannot get git commit hash short version: %s',
                           build.revision)
            revision = None

        if build.store_upload_status == 'Uploaded':
            store_rev = build.store_upload_revision
        else:
            store_rev = build.store_upload_error_message

        row = [
            recipe.name,
            ', '.join(build.store_channels),
            series_arch,
            build.buildstate,
            age,
            revision,
            store_rev,
            build_log,
        ]
        self.t.add_row(row)

    def generate(self):
        print(self.t.get_string(sort_key=operator.itemgetter(0, 1, 2),
                                sortby="Recipe Name"))


class JSONBuildsReport(BaseBuildsReport):
    def __init__(self, output):
        super().__init__(output)
        self.builds = collections.defaultdict(dict)

    def add_build(self, charm, recipe, build):
        build_arch_tag = build.distro_arch_series.architecture_tag
        series_arch = f'{build.distro_series.name}/{build_arch_tag}'
        self.builds[recipe.name][series_arch] = {
            'datebuilt': build.datebuilt,
            'store_channels': recipe.store_channels,
            'buildstate': build.buildstate,
            'build_log_url': build.build_log_url,
            'revision': build.revision_id,
            'store_upload_revision': build.store_upload_revision,
            'store_upload_status': build.store_upload_status,
            'store_upload_error_message': build.store_upload_error_message,
            'web_link': build.web_link,
            'recipe_web_link': recipe.web_link,
        }

    def generate(self):
        print(json.dumps(self.builds, default=str))


_build_report_klasses = {
    'html': HtmlBuildsReport,
    'plain': PlainBuildsReport,
    'json': JSONBuildsReport,
}


def get_builds_report_klass(kind: str) -> BaseBuildsReport:
    """Get a report class for a kind of output.

    :param kind: type of output report.
    :returns: a report class.
    """
    return _build_report_klasses[kind]


def get_supported_report_types() -> List[str]:
    """Get list of support report types.

    :returns: list of supported report types.
    """
    return list(_build_report_klasses.keys())