import logging
from multiprocessing import Process

from file_crawler_filter_util import exclude_generator


class FileCrawlerDirProcess(Process):
    def __init__(self, cli_args, dir_queue, file_queue, results):
        Process.__init__(self)
        self.__logger = logging.getLogger('file_crawler.FileCrawlerDirProcess')
        self.__cli_args = cli_args
        self.__dir_queue = dir_queue
        self.__file_queue = file_queue
        self.__results = results

    def run(self):
        while True:
            if not self.__dir_queue.empty():
                directory = self.__dir_queue.get()
                self._add_files_to_queue(directory)
                self.__dir_queue.task_done()

    def _add_files_to_queue(self, directory):
        dir_name = directory['dir_name']
        files = directory['files']
        self.__logger.debug("Processing directory %s" % dir_name)

        for file_path in exclude_generator(self.__cli_args, dir_name, files, True, self.__results):
            self.__file_queue.put(file_path)

        self.__logger.debug("Done adding %d files in directory %s" % (len(files), dir_name))