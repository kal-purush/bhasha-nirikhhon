import reader


def makeDatabase(filename):
    open(filename, "w").write("")


def addRow(name, value, filename):
    _newRow = reader.Row()
    _newRow.Name = name
    _newRow.Value = value

    reader.saveTable([_newRow], filename, False)


def changeRow(name, newValue, filename, overwrite):
    _contents = reader.readDbAsTable(filename)
    for row in _contents:
        if row.Name == name:
            row.Value = newValue

    reader.saveTable(_contents, filename, overwrite)


def dbAsString(filename):
    return open(filename, "r").read()


def dbAsTableOfRows(filename):
    return reader.readDbAsTable(filename)