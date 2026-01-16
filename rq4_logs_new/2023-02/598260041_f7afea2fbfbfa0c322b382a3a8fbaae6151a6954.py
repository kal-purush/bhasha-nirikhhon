from tensorflow.keras.preprocessing.image import ImageDataGenerator
import tensorflow as tf


def preprocess_input(x):
    x = x.astype('float32')
    x /= 255.0
    x -= 0.5
    x *= 2.0
    return x


def get_datagen(cfg):
    dataset_cfg = cfg["dataset"]
    img_size = dataset_cfg["input_shape"]
    augment = dataset_cfg["augment"]
    if augment:
        train_datagen = ImageDataGenerator(featurewise_center=False,
                                           samplewise_center=False,
                                           featurewise_std_normalization=False,
                                           samplewise_std_normalization=False,
                                           preprocessing_function=eval(dataset_cfg["preprocessing_function"]),
                                           rotation_range=30,
                                           width_shift_range=0.,
                                           height_shift_range=0.,
                                           shear_range=0.,
                                           zoom_range=0.2,
                                           channel_shift_range=0.,
                                           fill_mode='nearest',
                                           cval=0.,
                                           horizontal_flip=True,
                                           vertical_flip=False,
                                           rescale=None,
                                           brightness_range=[0.6, 1.2])
    else:
        train_datagen = ImageDataGenerator(preprocessing_function=eval(dataset_cfg["preprocessing_function"]),
                                           featurewise_center=False,
                                           samplewise_center=False,
                                           featurewise_std_normalization=False,
                                           samplewise_std_normalization=False,
                                           rotation_range=0.,
                                           width_shift_range=0.,
                                           height_shift_range=0.,
                                           shear_range=0.,
                                           zoom_range=0.,
                                           channel_shift_range=0.,
                                           fill_mode='nearest',
                                           cval=0.,
                                           horizontal_flip=True,
                                           vertical_flip=False,
                                           rescale=None)

    val_datagen = ImageDataGenerator(preprocessing_function=eval(dataset_cfg["preprocessing_function"]),
                                       featurewise_center=False,
                                       samplewise_center=False,
                                       featurewise_std_normalization=False,
                                       samplewise_std_normalization=False,
                                       rotation_range=0.,
                                       width_shift_range=0.,
                                       height_shift_range=0.,
                                       shear_range=0.,
                                       zoom_range=0.,
                                       channel_shift_range=0.,
                                       fill_mode='nearest',
                                       cval=0.,
                                       horizontal_flip=True,
                                       vertical_flip=False,
                                       rescale=None)

    train_generator = train_datagen.flow_from_directory(dataset_cfg["train_path"],
                                                  target_size=(img_size, img_size),
                                                  batch_size=dataset_cfg["batch_size"],
                                                  classes = ['coast', 'forest', 'highway', 'inside_city', 'mountain',
                                                             'Opencountry', 'street', 'tallbuilding'],
                                                  class_mode='categorical')


    validation_generator = val_datagen.flow_from_directory(dataset_cfg["val_path"],
                                                       target_size=(img_size, img_size),
                                                       batch_size=dataset_cfg["batch_size"],
                                                       classes=['coast', 'forest', 'highway', 'inside_city',
                                                                'mountain', 'Opencountry', 'street', 'tallbuilding'],
                                                       class_mode='categorical')

    return train_generator, validation_generator