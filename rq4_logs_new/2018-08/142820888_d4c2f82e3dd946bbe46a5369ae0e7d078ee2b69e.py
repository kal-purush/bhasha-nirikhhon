import tensorflow as tf
import keras as K

def keras_share_gpu():
    """
    Por padrão, O Keras/TensorFlow reserva toda a memória de GPU disponivel.
    Isso impede que mais de um treino seja feito simultaneamente.
    Chamando essa função, o Keras/TensorFlow vai alocar a memória conforma necessário.

    No caso do treinamento de textgenrnn, o uso foi de 5.5GB para 413MB
    """
    tf_config = tf.ConfigProto()
    tf_config.gpu_options.allow_growth = True  # dynamically grow the memory used on the GPU
    tf_config.log_device_placement = True  # to log device placement (on which device the operation ran)
                                        # (nothing gets printed in Jupyter, only if you run it standalone)
    K.backend.tensorflow_backend.set_session(tf.Session(config=tf_config))