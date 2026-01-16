from keras.callbacks import Callback
import numpy as np


class LogEpochStats(Callback):
    def __init__(self, steps_per_epoch, logger):
        super(LogEpochStats, self).__init__()
        self.steps_per_epoch = steps_per_epoch
        self.logger = logger

    def on_epoch_begin(self, epoch, logs=None):
        logs = logs or {}
        self.logger.info('Epoch {} started.'.format(epoch + 1))

    def on_batch_end(self, batch, logs=None):
        logs = logs or {}
        self.logger.info('Batch {}/{}, \tAccuracy -> {}, \t'
                    'Loss -> {}'
                    .format(batch, self.steps_per_epoch, logs.get('acc'), logs.get('loss')))

    def on_epoch_end(self, epoch, logs=None):
        logs = logs or {}
        self.logger.info('Epoch {} ended.'.format(epoch + 1))
        self.logger.info('Train accuracy -> {}, Train Loss -> {}, Validation Accuracy -> {}, Validation loss -> {}'
                    .format(logs.get('acc'), logs.get('loss'), logs.get('val_acc'), logs.get('val_loss')))



class CustomModelCheckpoint(Callback):
    def __init__(self, custom_model, filepath, monitor='val_loss', verbose=0,
                 save_best_only=False, save_weights_only=False,
                 mode='auto', period=1):
        super(CustomModelCheckpoint, self).__init__()
        self.monitor = monitor
        self.verbose = verbose
        self.filepath = filepath
        self.save_best_only = save_best_only
        self.save_weights_only = save_weights_only
        self.period = period
        self.epochs_since_last_save = 0
        self.custom_model = custom_model

        if mode not in ['auto', 'min', 'max']:
            # warnings.warn('ModelCheckpoint mode %s is unknown, '
            #               'fallback to auto mode.' % (mode),
            #               RuntimeWarning)
            mode = 'auto'

        if mode == 'min':
            self.monitor_op = np.less
            self.best = np.Inf
        elif mode == 'max':
            self.monitor_op = np.greater
            self.best = -np.Inf
        else:
            if 'acc' in self.monitor or self.monitor.startswith('fmeasure'):
                self.monitor_op = np.greater
                self.best = -np.Inf
            else:
                self.monitor_op = np.less
                self.best = np.Inf

    def on_epoch_end(self, epoch, logs=None):
        logs = logs or {}
        self.epochs_since_last_save += 1
        if self.epochs_since_last_save >= self.period:
            self.epochs_since_last_save = 0
            filepath = self.filepath.format(epoch=epoch + 1, **logs)
            if self.save_best_only:
                current = logs.get(self.monitor)
                if current is None:
                    pass
                    # warnings.warn('Can save best model only with %s available, '
                    #               'skipping.' % (self.monitor), RuntimeWarning)
                else:
                    if self.monitor_op(current, self.best):
                        if self.verbose > 0:
                            print('Epoch %05d: %s improved from %0.5f to %0.5f,'
                                  ' saving model to %s'
                                  % (epoch + 1, self.monitor, self.best,
                                     current, filepath))
                        self.best = current
                        if self.save_weights_only:
                            self.custom_model.save_weights(filepath, overwrite=True)
                        else:
                            self.custom_model.save(filepath, overwrite=True)
                    else:
                        if self.verbose > 0:
                            print('Epoch %05d: %s did not improve' %
                                  (epoch + 1, self.monitor))
            else:
                if self.verbose > 0:
                    print('Epoch %05d: saving model to %s' % (epoch + 1, filepath))
                if self.save_weights_only:
                    self.custom_model.save_weights(filepath, overwrite=True)
                else:
                    self.custom_model.save(filepath, overwrite=True)