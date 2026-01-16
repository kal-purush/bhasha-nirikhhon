from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *

import helperFunctions


class EditCurrentValuesDlg(QDialog):

    # ToDo: check to see if it gracefully handles all forms of user input

    def __init__(self, current, parent=None):
        super(EditCurrentValuesDlg, self).__init__(parent)
        self.current = current
        self.setup()

    def setup(self):
        self.resize(260, 150)

        pledgeLabel = QLabel("Current Pledge:")
        self.pledgeEdit = QLineEdit()
        self.pledgeEdit.setText(helperFunctions.decimalFormat(self.current['pledged'], 'dollars'))
        self.pledgeEdit.setToolTip('Enter the current pledge.')
        self.pledgeEdit.setWhatsThis('This is where the current pledge is entered. It is used to create the \'Pledged\' +'
                                   'part of the graphic.')
        pledgeBoxLayout = QHBoxLayout()
        pledgeBoxLayout.addStretch()
        pledgeBoxLayout.addWidget(pledgeLabel)
        pledgeBoxLayout.addWidget(self.pledgeEdit)

        collectedLabel = QLabel("Amount Collected:")
        self.collectedEdit = QLineEdit()
        self.collectedEdit.setText(helperFunctions.decimalFormat(self.current['collected'], 'dollars'))
        self.collectedEdit.setToolTip("Enter the amount currently collected for BAA.")
        self.collectedEdit.setWhatsThis('This is where the current amount collected is entered. It is used to create ' +
                                        'the \'Amount Collected\' part of the graphic. You may enter it formatted ' +
                                        'as dollars and cents ($12,345.67) or simply enter the digits (12345.67).')
        collectedBoxLayout = QHBoxLayout()
        collectedBoxLayout.addStretch()
        collectedBoxLayout.addWidget(collectedLabel)
        collectedBoxLayout.addWidget(self.collectedEdit)

        familiesLabel = QLabel("Total Family Count:")
        self.familiesEdit = QLineEdit()
        self.familiesEdit.setText(str(self.current["families"]))
        self.familiesEdit.setToolTip('Enter the number of families who have contributed so far.')
        self.familiesEdit.setWhatsThis('This is where the number of families currently participating is entered ' +
                                       'This is used to display the percentage of families participating in this ' +
                                       'year\'s Bishop\'s Annual Appeal.')
        familiesBoxLayout = QHBoxLayout()
        familiesBoxLayout.addStretch()
        familiesBoxLayout.addWidget(familiesLabel)
        familiesBoxLayout.addWidget(self.familiesEdit)

        dlgButtonBox = QDialogButtonBox(QDialogButtonBox.Cancel|QDialogButtonBox.Ok, Qt.Horizontal, self)
        dlgButtonBox.accepted.connect(self.accept)
        dlgButtonBox.rejected.connect(self.reject)
        buttonLayout = QHBoxLayout()
        buttonLayout.addStretch()
        buttonLayout.addWidget(dlgButtonBox)

        layout = QVBoxLayout(self)
        layout.addLayout(pledgeBoxLayout)
        layout.addLayout(collectedBoxLayout)
        layout.addLayout(familiesBoxLayout)
        layout.addStretch()
        layout.addLayout(buttonLayout)

    def getCurrent(self):
        return self.targets

    def accept(self):

        class PledgeError(Exception):pass

        class CollectedError(Exception):pass

        class FamiliesError(Exception):pass

        pledge = self.pledgeEdit.text()
        collected = self.collectedEdit.text()
        families = self.familiesEdit.text()
        try:
            if len(pledge) == 0:
                raise PledgeError("Please enter a value for the current pledge.")

            if len(collected) == 0:
                raise CollectedError('Please enter an amount collected so far.')
            if helperFunctions.String2Num(collected) < 0.0:
                raise CollectedError('The amount collected must be greater than or equal to zero.')
            try:
                helperFunctions.testForNumbers(collected, 'float')
            except ValueError:
                raise CollectedError('The amount collected must be a numeric value.')

            if len(families) == 0:
                raise FamiliesError('You must enter the number of families\n' +
                                    'currently participating.')
            if int(helperFunctions.String2Num(families)) < 0:
                raise FamiliesError('The number of participating families must be zero or more.')

            try:
                helperFunctions.testForNumbers(families, 'int')
            except ValueError:
                raise FamiliesError('The number of families must be\n' +
                                    'an integer.  Example: 1403')
        except PledgeError as e:
            response = QMessageBox.warning(self, "Pledge Error", str(e))
            self.yearEdit.selectAll()
            self.yearEdit.setFocus()
            return
        except CollectedError as e:
            QMessageBox.warning(self, "Goal Error", str(e))
            self.collectedEdit.selectAll()
            self.collectedEdit.setFocus()
            return
        except FamiliesError as e:
            QMessageBox.warning(self, "Families Error", str(e))
            self.familiesEdit.selectAll()
            self.familiesEdit.setFocus()
            return

        self.current['pledge'] = pledge
        self.current['collected'] = float(helperFunctions.cleanNumber(collected))
        self.current['families'] = int(helperFunctions.cleanNumber(families))

        QDialog.accept(self)

# End of EditCurrentValuesDlg Class---------------------------------------------------