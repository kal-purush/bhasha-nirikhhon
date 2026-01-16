import numpy as np

from motion_estimation._2D.farneback_OpenCV import OF_Estimation
from motion_estimation._2D.project import Projection

import logging
logging.basicConfig(format="[%(filename)s:%(lineno)s %(funcName)s()] %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

estimator = OF_Estimation(logger)
projector = Projection(logger)

def shake(x, y, std_dev=1.0):
    """
    Apply Gaussian noise to the given indices and return a sorted mapping.
    """
    displacements = np.random.normal(0, std_dev, len(x))
    return np.stack((y + displacements, x), axis=1)

def randomize_image(image, std_dev=16.0):
    """
    Apply a controlled randomization to a 2D image.
    This perturbs pixel positions along the X and Y axes while maintaining structure.
    """
    randomized_image = np.empty_like(image)
    
    # Randomization in X (Columns)
    values = np.arange(image.shape[1])  # Column indices
    for y in range(image.shape[0]):  # Iterate over rows
        pairs = shake(values, np.arange(len(values)), std_dev).astype(np.int32)
        pairs = pairs[pairs[:, 0].argsort()]  # Sort based on perturbed positions
        randomized_image[y, values] = image[y, pairs[:, 1]]  # Apply reordering

    # Randomization in Y (Rows)
    values = np.arange(image.shape[0])  # Row indices
    for x in range(image.shape[1]):  # Iterate over columns
        pairs = shake(values, np.arange(len(values)), std_dev).astype(np.int32)
        pairs = pairs[pairs[:, 0].argsort()]  # Sort based on perturbed positions
        randomized_image[values, x] = randomized_image[pairs[:, 1], x]  # Apply reordering

    return randomized_image

def project_A_to_B(A, B, estimator):
  zeros = np.zeros((A.shape[0], A.shape[1], 2), dtype=np.float32)
  MVs = estimator.pyramid_get_flow(target=B, reference=A, flow=zeros)
  projection = projector.remap(A, MVs)
  return projection

def randomize_and_project(image, std_dev=16.0, estimator):
  randomized_image = randomize_image(image, std_dev)
  projection = project_A_to_B(randomized_image, image, estimator) # Ojo, pueden estar al revés
  return projection