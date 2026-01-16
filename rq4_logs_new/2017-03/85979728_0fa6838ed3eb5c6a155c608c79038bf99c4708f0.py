import numpy as np
import tensorflow as tf
import datetime
import data_helpers
import train_cnn
from sent_cnn import SentCNN
import config

x_u_i, x_r_i, y, max_len, U = train_cnn.load_data(config.config)

train_cnn.train_cnn(x_u_i, x_r_i, y, max_len, U, config.config,debug=False)