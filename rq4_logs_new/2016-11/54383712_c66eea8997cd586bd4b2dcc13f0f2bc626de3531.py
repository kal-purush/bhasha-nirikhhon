# -*- coding: utf-8 -*-

import os
import csv
import sys
import types
from UserDict import UserDict

from DateTime import DateTime

from zope.event import notify
from zope.interface import implements

from plone.app.layout.globals.interfaces import IViewView
from plone.protect import CheckAuthenticator

from collective.progressbar.events import ProgressBar
from collective.progressbar.events import ProgressState
from collective.progressbar.events import UpdateProgressEvent
from collective.progressbar.events import InitialiseProgressBar

from Products.Five.browser.pagetemplatefile import ViewPageTemplateFile
from Products.statusmessages.interfaces import IStatusMessage
from Products.CMFPlone.utils import transaction_note
from Products.CMFPlone.utils import _createObjectByType

from bika.lims.utils import tmpID
from bika.lims.browser import BrowserView
from bika.lims import bikaMessageFactory as _

from bika.uw import logger


class ClientARImportAddView(BrowserView):
    implements(IViewView)
    template = ViewPageTemplateFile('templates/arimport_add_form.pt')

    def __init__(self, context, request):
        super(ClientARImportAddView, self).__init__(context, request)
        self.context = context
        self.request = request

    def __call__(self):
        request = self.request
        CheckAuthenticator(self.request.form)

        if self.form_get('submitted'):
            csvfile = self.form_get('csvfile')
            arimport, msg = self._import_file(csvfile)

            if arimport:
                msg = "AR Import complete"
                self.statusmessage(_(msg), "info")
                request.response.write(
                    '<script>document.location.href="%s"</script>' % (
                        arimport.absolute_url()))
                return
            else:
                self.statusmessage(_(msg), "error")
                request.response.write(
                    '<script>document.location.href="%s/arimport_add"</script>' % (self.context.absolute_url()))
                return

        return self.template()

    def _import_file(self, csvfile):
        """Import the CSV file.
        """
        # Initialize the Progress Bar
        self.progressbar_init("Importing File")

        # parse the CSV data into the interanl data structure
        import_data = ImportData(csvfile)

        # Validate the data
        valid = import_data.validate()
        if type(valid) in types.StringTypes:
            transaction_note(valid)
            return None, valid

        # XXX: Why do we have to do this?
        #      Are we not always in a client folder
        client_id = self.form_get("ClientID")
        client = self.get_client_by_id(client_id)
        if client is None:
            # This is not a user input issue - client_id is added to template
            return None, "Could not find Client {0}".format(client_id)

        # Note: The variable names refer to the spreadsheet cells, e.g.:
        #       _2B is the cell at column B row 2.

        # File name, Client name, Client ID, Contact, Client Order Number, Client Reference
        _2B, _2C, _2D, _2E, _2F, _2G = import_data.get_data("header")

        # List of sample data rows (lists)
        sample_data = import_data.get_sample_data()

        # Create an ARImport object
        arimport = self.create_ar_import_obj(
            folder=client,
            title=_2B,
            FileName=_2C,
            ClientTitle=_2D,
            ClientID=_2E,
            ClientOrderNumber=_2F,
            ClientReference=_2G,
            NumberSamples=len(sample_data),
            DateImported=DateTime(),
        )

        # DateSampled, Media, SamplePoint, Activity Sampled
        _9B, _9C, _9D, _9E = import_data.get_data("samples")

        for n, sample in enumerate(sample_data):
            # ClientSampleID, Amount Sampled, Metric, Remarks
            _xB, _xC, _xD, _xE = sample
            self.create_ar_import_item_obj(
                folder=arimport,
                ClientSid=_xB,
                SampleDate=DateTime(_9B),
                ClientRef=_2G,
                ClientRemarks=_xE,
            )

            # progress
            self.progressbar_progress(n, len(sample_data))

        return arimport, "Success"

    def create_ar_import_item_obj(self, folder=None, id=None, title=None, **kwargs):
        """Create a new ARImportItem object
        """
        if id is None:
            id = '%s_%s' % ('aritem', tmpID())
        ar = _createObjectByType("ARImportItem", folder, id, title=title, **kwargs)
        ar._renameAfterCreation()
        return ar

    def create_ar_import_obj(self, folder=None, id=None, title=None, **kwargs):
        """Create a new ARImport object
        """
        if id is None:
            id = tmpID()
        # Add a postfix index to the title
        postfix = 1
        while title in [i.Title() for i in folder.objectValues()]:
            title = '%s-%s' % (title, postfix)
            postfix += 1
        ar = _createObjectByType("ARImport", folder, id, title=title, **kwargs)
        ar._renameAfterCreation()
        return ar

    def progressbar_init(self, title):
        """Progress Bar
        """
        bar = ProgressBar(self.context, self.request, title, description='')
        notify(InitialiseProgressBar(bar))

    def progressbar_progress(self, n, total):
        """Progres Bar Progress
        """
        progress_index = float(n) / float(total) * 100.0
        progress = ProgressState(self.request, progress_index)
        notify(UpdateProgressEvent(progress))

    def get_client_by_id(self, id):
        """Search the Client by the given ID
        """
        results = self.portal_catalog(portal_type='Client', id=id)
        if results:
            return results[0].getObject()
        return None

    def statusmessage(self, msg, facility="info"):
        """Add a statusmessage to the response
        """
        return IStatusMessage(self.request).addStatusMessage(msg, facility)

    def form_get(self, key):
        """Get the value from the form
        """
        form = self.request.form
        value = form.get(key)
        logger.debug("key={0} -> value={1}".format(key, value))
        return value


class ImportData(UserDict):
    """Dictionary wrapper to hold the spreadsheet data.
    """
    data = {
        "header": {
            "fields": [],
            "data": [],
        },
        "batch_header": {
            "fields": [],
            "data": [],
        },
        "analytes": {
            "fields": [],
            "data": [],
        },
        "samples": {
            "fields": [],
            "data": [],
        },
        "client_samples": {
            "fields": [],
            "data": [],
        },
    }

    def __init__(self, csvfile, delimiter=";", quotechar="'"):
        logger.debug("ImportData::__init__")
        self.csvfile = csvfile
        self.reader = csv.reader(csvfile, delimiter=delimiter, quotechar="'")
        self.parse()

    def parse(self):
        """Parse the CSV to the internal data structure
        """
        success = True

        section = None
        for n, row in enumerate(self.reader):
            # we use row[0] as an identifier to match the key in the data dict, e.g.
            # Batch Header -> batch_header
            identifier = row[0].replace(" ", "_").lower()

            # get the row data (column 1 to the end of the line)
            row_data = self.get_row_data(row)

            # skip empty cells
            if row_data is None:
                continue

            # check if the current identifier matches a key in the data dict
            if identifier in self.data:
                # a new section begins
                section = identifier
                # remember the fields
                self.data[identifier]["fields"] = row_data
                # XXX: "Analytes" section contains only a single row (should be
                #      fixed in the spreadsheet)
                if identifier == "analytes":
                    self.data[identifier]["data"] = row_data
                # go to the next row
                continue

            # XXX: custom handling for the "Samples" section (should be fixed in the spreadsheet)
            if section == "samples" and row[1] == "ClientSampleID":
                # manually set the section
                section = "client_samples"
                # and add the rest of the row to the fields
                self.data["client_samples"]["fields"] = row_data
                continue

            try:
                # append the row data
                self.data[section]["data"].append(row_data)
            except KeyError:
                logger.error("Invalid Identifier {0} in Line {1}".format(row[0], n + 1))
                success = False
                # XXX: Mark the whole set as invalid or continue?
                continue
        return success

    def get_row_data(self, row, start=1, remove_trailing_empty=True):
        end = len(row)
        if remove_trailing_empty:
            sys.setrecursionlimit(2000)
            # we iterate from the end of the list to the start to remove
            # empty cells
            rev = reversed(row)
            for n, i in enumerate(rev):
                if n + 1 == len(row):
                    return None
                if i != "":
                    return row[start:end - n]
        return row[start:end]

    def validate(self):
        """Validate the data structure
        """
        # Validate the sections
        for section in self.keys():
            logger.info("Processing Section {0}".format(section))

            fields = self.get_fields(section)
            data = self.get_data(section)

            if not fields:
                return "Invalid section {0}: No fields".format(section)
            if not data:
                return "Invalid section {0}: No data".format(section)

        # Validate the filename
        # XXX: Why do we need to do this?

        csv_filename = self.get_csv_filename()
        data_filename = self.get_data_filename()

        if csv_filename.lower() != data_filename.lower():
            return "Filename '{0}' does not match entered filename '{1}'".format(
                csv_filename, data_filename)

        return True

    def get_csv_filename(self):
        """Return the filename of the CSV
        """
        filename = os.path.basename(self.csvfile.filename)
        return os.path.splitext(filename)[0]

    def get_data_filename(self):
        """Return the filename of the "Header" section
        """
        header = self.get_data("header")
        return header[0]

    def get_fields(self, section):
        """Fields accessor for the named section
        """
        return self.data[section]["fields"]

    def get_data(self, section):
        """Data accessor for the named section
        """
        data = self.data[section]["data"]
        if len(data) == 1:
            return data[0]
        return data

    def get_sample_data(self):
        """Return the data of the "Samples" section
        """
        return self.get_data("client_samples")