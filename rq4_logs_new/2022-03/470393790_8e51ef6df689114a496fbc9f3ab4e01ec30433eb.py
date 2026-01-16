import argparse
import os
import numpy as np
from tqdm import tqdm
import cv2
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Conv2D, Reshape, Flatten
from tensorflow.keras.layers import Dropout, LeakyReLU, BatchNormalization
from tensorflow.keras.layers import Activation, ZeroPadding2D, UpSampling2D
from tensorflow.keras.layers import Reshape

# Load real art dataset
def load_dataset(dataset_path, img_size):
    print('loading data...\n')
    data = []
    images = os.listdir(dataset_path)
    print(f'Number of artworks found: {len(images)}')
    print('Processing images, this will take a few minutes........')
    for img in tqdm(images, ncols=100):
        img_path = os.path.join(dataset_path, img)
        img_array = cv2.imread(img_path)
        resized_img = cv2.resize(img_array, img_size)
        data.append(resized_img / 255)
    np.random.shuffle(data)
    print('Processing succeeded!')
    return data

# Save generated images
def save_images(generator, noise, output_path):
    generated_images =  generator.predict(noise)
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    for index, img in enumerate(generated_images):
        img_array = img * 255
        
        filename = os.path.join(output_path, f"{index + 1}.jpg")
        cv2.imwrite(filename, img_array)

# Build generator model
def build_generator(seed_size, channels):
    model = Sequential()

    model.add(Dense(64*64, activation='relu', input_dim=seed_size)) #64x64 units
    model.add(Reshape((4, 4, 256)))

    model.add(UpSampling2D())
    model.add(Conv2D(256, kernel_size=3, padding='same'))
    model.add(BatchNormalization(momentum=0.8))
    model.add(Activation('relu'))
    
    model.add(UpSampling2D())
    model.add(Conv2D(256, kernel_size=3, padding='same'))
    model.add(BatchNormalization(momentum=0.8))
    model.add(Activation('relu'))
    
    model.add(Conv2D(256, kernel_size=3, padding='same'))
    model.add(BatchNormalization(momentum=0.8))
    model.add(Activation('relu'))
   
    model.add(UpSampling2D(size=(2,2)))
    model.add(Conv2D(256, kernel_size=3, padding='same'))
    model.add(BatchNormalization(momentum=0.8))
    model.add(Activation('relu'))
    
    model.add(UpSampling2D(size=(2,2)))
    model.add(Conv2D(256, kernel_size=3, padding='same'))
    model.add(BatchNormalization(momentum=0.8))
    model.add(Activation('relu'))

    model.add(Conv2D(channels, kernel_size=3, padding='same'))
    model.add(Activation('tanh'))
    
    return model

# Build discriminator model
def build_discriminator(image_shape):
    model = Sequential()

    model.add(Conv2D(32, kernel_size=3, strides=2, input_shape=image_shape, padding='same'))
    model.add(LeakyReLU(alpha=0.2))

    model.add(Dropout(0.25))
    model.add(Conv2D(64, kernel_size=3, strides=2, padding='same'))
    model.add(ZeroPadding2D(padding=((0,1), (0,1))))
    model.add(BatchNormalization(momentum=0.8))
    model.add(LeakyReLU(alpha=0.2))

    model.add(Dropout(0.25))
    model.add(Conv2D(128, kernel_size=3, strides=2, padding='same'))
    model.add(BatchNormalization(momentum=0.8))
    model.add(LeakyReLU(alpha=0.2))

    model.add(Dropout(0.25))
    model.add(Conv2D(256, kernel_size=3, padding='same'))
    model.add(BatchNormalization(momentum=0.8))
    model.add(LeakyReLU(alpha=0.2))

    model.add(Dropout(0.25))
    model.add(Conv2D(512, kernel_size=3, padding='same'))
    model.add(BatchNormalization(momentum=0.8))
    model.add(LeakyReLU(alpha=0.2))

    model.add(Dropout(0.25))
    model.add(Flatten())
    model.add(Dense(1, activation='sigmoid'))
    
    return model

def discriminator_loss(real_output, fake_output):
    cross_entropy = tf.keras.losses.BinaryCrossentropy()
    real_loss = cross_entropy(tf.ones_like(real_output), real_output)
    fake_loss = cross_entropy(tf.zeros_like(fake_output), fake_output)
    total_loss = real_loss + fake_loss
    return total_loss

def generator_loss(fake_output):
    cross_entropy = tf.keras.losses.BinaryCrossentropy()
    return cross_entropy(tf.ones_like(fake_output), fake_output)

@tf.function
def train_step(images, generator, discriminator, generator_optimizer, discriminator_optimizer, seed_size, batch_size):
    seed = tf.random.normal([batch_size, seed_size])
    
    with tf.GradientTape() as gen_tape, tf.GradientTape() as disc_tape:
        generated_images = generator(seed, training=True)
        
        real_output = discriminator(images, training=True)
        fake_output = discriminator(generated_images, training=True)
        
        gen_loss = generator_loss(fake_output)
        disc_loss = discriminator_loss(real_output, fake_output)
        
        
        gradients_of_generator = gen_tape.gradient(
            gen_loss,
            generator.trainable_variables
        )
        gradients_of_discriminator = disc_tape.gradient(
            disc_loss, 
            discriminator.trainable_variables
        )
        
        generator_optimizer.apply_gradients(zip(gradients_of_generator,
                                                generator.trainable_variables))
        
        discriminator_optimizer.apply_gradients(zip(gradients_of_discriminator, 
                                                    discriminator.trainable_variables))
        
        return gen_loss, disc_loss

def train(dataset, generator, discriminator, generator_optimizer, discriminator_optimizer, seed_size, batch_size, epochs, generator_weights, discriminator_weights):
    for epoch in range(epochs):
        gen_loss_list = []
        disc_loss_list = []
        
        for image_batch in dataset:
            t = train_step(image_batch, generator, discriminator, generator_optimizer, discriminator_optimizer, seed_size, batch_size)
            gen_loss_list.append(t[0])
            disc_loss_list.append(t[1])
            
        g_loss = sum(gen_loss_list) / len(gen_loss_list) #calculate losses
        d_loss = sum(disc_loss_list) / len(disc_loss_list)
        
        print(f'Epoch {epoch+1}, gen loss = {g_loss}, disc loss = {d_loss}')
    
    discriminator.save_weights(discriminator_weights)
    generator.save(generator_weights)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=str, default='dataset', help='dataset path')
    parser.add_argument('--output-dir', type=str, default='generated_arts', help='output path')
    parser.add_argument('--epochs', type=int, default=250, help='number of epochs')
    parser.add_argument('--batch-size', type=int, default=32, help='total batch size for all GPUs')
    parser.add_argument('--seed-size', type=int, default=100, help='number of output images')
    parser.add_argument('--image-channels', type=int, default=3, help='number of channels in images (3 for rgb images, 1 for gray images)')
    parser.add_argument('--generator-weights', type=str, default='generator_weights.h5', help='generator model weights path')
    parser.add_argument('--discriminator-weights', type=str, default='discriminator_weights.h5', help='discriminator model weights path')
    opt = parser.parse_args()

    # set variables
    dataset_path = opt.data
    output_path = opt.output_dir
    epochs = opt.epochs
    batch_size = opt.batch_size
    seed_size = opt.seed_size
    image_channels = opt.image_channels
    generator_weights = opt.generator_weights
    discriminator_weights = opt.discriminator_weights

    # constants
    WIDTH = 64
    HEIGHT = 64
    IMG_SIZE = (WIDTH, HEIGHT)

    # build generator
    generator = build_generator(seed_size, image_channels)

    # build discriminator
    image_shape = (WIDTH, HEIGHT, image_channels)
    discriminator = build_discriminator(image_shape)

    try:
        # load weights
        generator.load_weights(generator_weights)
        discriminator.load_weights(discriminator_weights)

        # generate images
        fixed_seed = np.random.normal(0, 1, (100, seed_size))
        save_images(generator, fixed_seed, output_path)
    except:
        # load training dataset
        data = load_dataset(dataset_path, IMG_SIZE)
        training_dataset = tf.data.Dataset.from_tensor_slices(data).batch(batch_size)

        # define optimizers
        generator_optimizer = tf.keras.optimizers.Adam(1.2e-4, 0.5)
        discriminator_optimizer = tf.keras.optimizers.Adam(1.5e-4, 0.5)

        # train the model and
        train(training_dataset, generator, discriminator, generator_optimizer, discriminator_optimizer, seed_size, batch_size, epochs, generator_weights, discriminator_weights)

        # generate images
        fixed_seed = np.random.normal(0, 1, (100, seed_size))
        save_images(generator, fixed_seed, output_path)