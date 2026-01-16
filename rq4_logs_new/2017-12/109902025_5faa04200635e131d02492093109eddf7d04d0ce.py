"""
Simple plugin that will allow for an easy means of registering activity logging.  Functionality is used by the timeline
plugin.
"""
from datetime import datetime, time

from collections import namedtuple

from autology import topics
from autology.configuration import add_default_configuration, get_configuration
from autology.publishing import publish
from autology.reports.models import Report
from autology.reports.timeline import keys as fm_keys
from autology.reports.timeline.processors import markdown as md_loader

DayReport = namedtuple('DayReport', 'date url num_entries')

_defined_plugins = []


def register_plugin():
    """
    Subscribe to the initialize method and add default configuration values to the settings object.
    :return:
    """
    topics.Application.INITIALIZE.subscribe(_initialize)

    add_default_configuration('simple', {
        'activities': []
    })


def _initialize():
    """
    Look in the configuration and create a new SimpleReportPlugin for all of the activities that are defined.
    :return:
    """
    for config in get_configuration().simple.activities:
        plugin = SimpleReportPlugin(config.id, config.name, config.description)
        plugin.initialize()
        _defined_plugins.append(plugin)


class SimpleReportPlugin:

    day_template_path = ['simple', 'day']
    index_template_path = ['simple', 'index']

    def __init__(self, _id, _name, _description):
        # The content that is stored for each individual day
        self._day_content = []

        # Dates that have been collected
        self._dates = []
        self.id = _id
        self.name = _name
        self.description = _description

    def initialize(self):
        """
        Register for all of the required events that will be fired off by the main loop
        :return:
        """
        topics.Processing.DAY_START.subscribe(self._start_day_processing)
        topics.Processing.PROCESS_FILE.subscribe(self._data_processor)
        topics.Processing.DAY_END.subscribe(self._end_day_processing)
        topics.Processing.END.subscribe(self._end_processing)

    def _start_day_processing(self, date=None):
        """
        Event handler that will be notified when a day's files are starting to be processed.
        :param date: the day that is being processed.
        :return:
        """
        self._day_content = []

    def test_activities(self, activities_list):
        return self.id in activities_list

    def _data_processor(self, entry):
        """
        Checks to see if the data can be processed and stores any data required locally.
        :param entry:
        :return:
        """
        if entry.mime_type is not md_loader.MIME_TYPE:
            return

        activities_list = entry.metadata.get(fm_keys.ACTIVITIES, [])
        if self.test_activities(activities_list):
            self._day_content.append(entry)

    def _end_day_processing(self, date=None):
        """Publish the content of the collated day together."""
        # Only if there is content to publish
        if self._day_content:
            url = publish(*self.day_template_path,
                          entries=sorted(self._day_content, key=lambda x: x.metadata[fm_keys.TIME]),
                          date=date, id=self.id, name=self.name, description=self.description)
            self._dates.append(DayReport(date=datetime.combine(date=date, time=time.min), url=url,
                                         num_entries=len(self._day_content)))

    def _end_processing(self):
        """All of the input files have been processed, so now need to build the master input value."""
        # Iterate through all of the values and count entries per day so can determine a decent legend value
        if self._dates:
            max_entries = max(self._dates, key=lambda x: x.num_entries).num_entries
            max_year = max(self._dates, key=lambda x: x.date).date.year
            min_year = min(self._dates, key=lambda x: x.date).date.year
        else:
            max_entries = 0
            max_year = min_year = datetime.now().year

        url = publish(*self.index_template_path, dates=self._dates, max_entries=max_entries,
                      max_year=max_year, min_year=min_year,
                      id=self.id, name=self.name, description=self.description)
        topics.Reporting.REGISTER_REPORT.publish(report=Report(self.name, self.description, url))