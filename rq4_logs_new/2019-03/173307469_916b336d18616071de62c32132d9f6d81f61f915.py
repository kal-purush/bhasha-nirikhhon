import logging
from tulip import tlp
import os
from tempfile import TemporaryDirectory, NamedTemporaryFile
from typing import Dict, List


from common.entities import OveMeta, WorkerType
from workers.base import BaseWorker


class NetworkWorker(BaseWorker):
    def worker_type(self) -> str:
        return WorkerType.TULIP.value

    def extensions(self) -> List:
        return ['.tlp', '.tlp.gz', '.tlpz', '.tlpb', '.tlpb.gz', '.tlpbz', '.json', '.gexf', '.net', '.paj', '.gml',
                '.dot', '.txt']

    def description(self) -> str:
        return "Applies a layout algorithm to a network using the Tulip framework"

    # TODO: do docs and parameters
    def docs(self) -> str:
        return "DeepZoomImageWorker.md"

    def parameters(self) -> Dict:
        return {}

    def process(self, project_name: str, meta: OveMeta, options: Dict):
        logging.info("Copying %s/%s into the temp place ...", project_name, meta.name)

        if not options:
            options = {}

        with TemporaryDirectory() as folder:

            with TemporaryDirectory() as folder2:
                network_file = os.path.join(folder2, meta.filename)
                open(network_file, 'a').close()

                self._file_controller.download_asset(project_name=project_name, asset_name=meta.name, filename=meta.file_location, down_filename=network_file)

                algorithm = options.get('algorithm')
                if not algorithm:
                    algorithm = "FM^3 (OGDF)"

                params = tlp.getDefaultPluginParameters(algorithm)
                for param in options.get('layout_params', []):
                    params[param] = options['layout_params'][param]

                graph = tlp.loadGraph(network_file)
                graph.applyLayoutAlgorithm(algorithm, params)

                result_name = options.get('result_name')
                if not result_name:
                    result_name = meta.name.split('.')[0] + '.gml'
                    tlp.saveGraph(graph, folder + '/' + result_name)

                    result_name = meta.name.split('.')[0] + '.svg'
                    tlp.saveGraph(graph, folder + '/' + result_name)
                else:
                    tlp.saveGraph(graph, folder + '/' + result_name)

                self._file_controller.upload_asset_folder(project_name=project_name, meta=meta, upload_folder=folder, worker_name=self.name)
        meta.index_file = meta.worker_root + self.name + "/" + os.path.splitext(os.path.basename(meta.filename))[0] + "/" + result_name
        self._file_controller.set_asset_meta(project_name, meta.name, meta)
        logging.info("Finished generating %s/%s into the storage ...", project_name, meta.name)