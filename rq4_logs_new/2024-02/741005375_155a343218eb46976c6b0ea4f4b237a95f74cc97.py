import os
import sys
import random
import torch
import unittest
from pathlib import Path

current_dir = Path(__file__).parent
parent_dir = current_dir.parent  # this should point to 'graphdiffusion/'
sys.path.insert(0, str(parent_dir))

from graphdiffusion.pipeline import PipelineEuclid

class TestPipelineEuclid(unittest.TestCase):
    def setUp(self):
        # Initialize your PipelineEuclid here with the desired parameters
        self.pipeline = PipelineEuclid(node_feature_dim=5)

    def test_train(self):
        example_tensor = torch.randn(5) * 10  # Replace with your actual data
        # You may need to mock the train method if it requires specific structure of data or parameters
        self.pipeline.train(example_tensor)
        # Include assertions here to validate the training process
        # For example: self.assertIsNotNone(result), if your train method returns a result

    def test_reconstruction(self):
        example_tensor = torch.randn(5) * 10
        x = self.pipeline.reconstruction(example_tensor, 0)
        # Include assertions here to validate the reconstruction
        self.assertIsNotNone(x)

    def test_bridge(self):
        data_prediction = torch.randn(5) * 10
        x = self.pipeline.bridge(data_now=0.5, data_prediction=data_prediction, t_now=1.0, t_query=0.5)
        # Include assertions here to validate the bridge functionality
        self.assertIsNotNone(x)

    def test_distance(self):
        x1 = torch.tensor([5.0 + random.random() * 0.01, -5.0 - random.random() * 0.01])
        x2 = torch.tensor([5.0 + random.random() * 0.01, -5.0 - random.random() * 0.01])
        distance = self.pipeline.distance(x1, x2)
        # Include assertions here to validate the distance functionality
        self.assertIsNotNone(distance)

    def test_inference(self):
        example_tensor = torch.randn(5,5) * 10
        data0 = self.pipeline.inference(data=example_tensor, noise_to_start=None)
        # Include assertions here to validate the inference
        self.assertIsNotNone(data0)

if __name__ == '__main__':
    unittest.main()