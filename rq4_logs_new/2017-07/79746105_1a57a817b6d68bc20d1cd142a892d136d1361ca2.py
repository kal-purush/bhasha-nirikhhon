# coding: utf-8

# Copyright (C) 2017 Open Path View, Maison Du Libre
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License along
# with this program. If not, see <http://www.gnu.org/licenses/>.

# Contributors: Benjamin BERNARD <benjamin.bernard@openpathview.fr>
# Email: team@openpathview.fr
# Description: Make it all, with correction logic

from opv_tasks.task import Task, TaskStatusCode
from opv_api_client import ressources, Filter
from opv_tasks.utils import find_task

class MakeallTask(Task):
    TASK_NAME = "makeall"
    requiredArgsKeys = ["id_cp", "id_malette"]

    def runTask(self, dm_c, db_c, task_name, inputData):
        """
        Run task.
        Return a TaskReturn.
        """
        Task = find_task(task_name)
        if not Task:
            raise Exception('Task %s not found' % task_name)

        task = Task(client_requestor=db_c, opv_directorymanager_client=dm_c)
        return task.run(options=inputData)

    def runWithExceptions(self, options={}):
        """
            :param options: {"id_lot": , "id_malette"}
            :return:
        """

        tasks = ["rotate", "cpfind", "autooptimiser", "stitchable", "stitch", "photosphere", "tiling"]
        inputData = options
        for task in tasks:
            self.logger.info("Starting task %s" % task)

            lastTaskReturn = self.runTask(self._opv_directory_manager, self._client_requestor, task, inputData)
            self.logger.debug("TaskReturn : " + lastTaskReturn.toJSON())
            inputData = lastTaskReturn.outputData

            if not lastTaskReturn.isSuccess():
                if task == "stitchable" and lastTaskReturn.statusCode == TaskStatusCode.ERROR_CP_APN0:
                    self.logger.info("APN0 error, injecting points ...")
                    toCp = lastTaskReturn.outputData
                    cp = self._client_requestor.make(ressources.Cp, toCp["id_cp"], toCp["id_malette"])
                    cp.get()
                    cp.lot.get()
                    self.logger.debug(cp.lot.id)
                    lastTaskReturn = self.runTask(self._opv_directory_manager, self._client_requestor, "findnearestcp", cp.lot.id)
                    self.logger.debug(lastTaskReturn.toJSON())
                    fromCp = lastTaskReturn.outputData

                    if not(fromCp is not None and "id_cp" in fromCp):
                        break

                    injectInput = {}
                    injectInput["idCpFrom"] = fromCp
                    injectInput["idCpTo"] = toCp
                    injectInput["apnList"] = [0]
                    self.logger.debug("injectInput: " + str(injectInput))
                    lastTaskReturn = self.runTask(self._opv_directory_manager, self._client_requestor, "injectcpapn", injectInput)
                    inputData = lastTaskReturn.outputData
                    self.logger.debug(lastTaskReturn.toJSON())

                    lastTaskReturn = self.runTask(self._opv_directory_manager, self._client_requestor, "autooptimiser", inputData)
                    inputData = lastTaskReturn.outputData
                    self.logger.debug(lastTaskReturn.toJSON())

                    lastTaskReturn = self.runTask(self._opv_directory_manager, self._client_requestor, "stitchable", inputData)
                    inputData = lastTaskReturn.outputData
                    self.logger.debug(lastTaskReturn.toJSON())

                    continue

                self.logger.error("Last task executed failled with following error : " + lastTaskReturn.error)
                break

            self.logger.info("End of task %s" % task)

        return inputData