from util import *


def listCmdHandler(request):
    path = request["path"]
    return listDir(path)


def getCmdHandler(request):
    fileName = request["fileName"]
    return getFileContent(fileName)


def putCmdHandler(request):
    fileName = request["fileName"]
    fileContent = request["fileContent"]

    return createFile(fileName, fileContent)