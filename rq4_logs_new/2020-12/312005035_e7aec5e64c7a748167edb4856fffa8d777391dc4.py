import os
import importlib
import inspect

from metric import Metric

class MetricsEngine():
	
	def __init__(self):
		self.metrics_list = []
		pass

	def load_metrics(self, path):
		plugins = os.listdir(path)

		for plugin in plugins:
			if '_metric.py' not in plugin:
				plugins.remove(plugin)
				continue
			module_str = f"{path.replace('/', '')}"
			spec = importlib.util.spec_from_file_location(module_str, f"{path}/{plugin}")
			module = importlib.util.module_from_spec(spec)
			spec.loader.exec_module(module)

			functions = inspect.getmembers(module, inspect.isfunction)
			name, function = functions[0]

			self.metrics_list.append(Metric(name, function))