
# coding: utf-8

# In[2]:


import tensorflow as tf
tf.enable_eager_execution()
import os
os.environ['CUDA_VISIBLE_DEVICES'] = '2'
print(os.environ['CUDA_VISIBLE_DEVICES'])


# In[3]:


import data
import importlib
importlib.reload(data)


# In[ ]:


annotation_file, PATH = data.download()


# In[4]:


annotation_file = './annotations/captions_train2014.json'
PATH = './train2014/'


# In[5]:


train_captions, img_name_vector, all_captions, all_img_name_vector = data.prune_dataset(annotation_file, 
                                                                                        path=PATH, 
                                                                                        num_examples=30000)


# In[ ]:


import pickle
# pickle.dump((train_captions, all_captions), open('captions.pickle', 'wb'))
pickle.dump((all_captions, train_captions), open('captions.pickle', 'wb'))
pickle.dump((img_name_vector, all_img_name_vector), open('img_names.pickle','wb'))


# In[6]:


import irenet
import importlib
importlib.reload(irenet)

# importlib.reload(image_encoder)


# In[ ]:


model.image_decoder.summary()


# In[7]:


model = irenet.IreNet(embedding_dim=512, max_vocab_size=5000)


# In[ ]:


import pickle
img_name_vector, all_name_vector = pickle.load(open('img_names.pickle', 'rb'))


# In[ ]:


data.cache_img_features(model.inception, img_name_vector, batch_size=100)


# In[ ]:


# Create training and validation sets using 80-20 split
img_name_train, img_name_val, cap_train, cap_val = train_test_split(img_name_vector, 
                                                                    cap_vector, 
                                                                    test_size=0.2, 
                                                                    random_state=0)


# In[8]:


dataset, tokenizer = data.get_tf_data(img_name_vector, train_captions, model.doc2vec, batch_size=128, buffer_size=1000, parallel_workers=8, top_k=5000)


# In[ ]:


irenet.train(model, dataset, tokenizer, batch_size=128, epochs=7)


# In[ ]:


optimizer = tf.train.AdamOptimizer()
img_encoder_checkpoint = tf.train.Checkpoint(optimizer=optimizer,
                           model=model.image_encoder,
                           optimizer_step=tf.train.get_or_create_global_step())
img_encoder_checkpoint.save('./checkpoints2/img_encoder')
img_decoder_checkpoint = tf.train.Checkpoint(optimizer=optimizer,
                           model=model.image_decoder,
                           optimizer_step=tf.train.get_or_create_global_step())
img_decoder_checkpoint.save('./checkpoints2/img_decoder')
txt_encoder_checkpoint = tf.train.Checkpoint(optimizer=optimizer,
                           model=model.text_encoder,
                           optimizer_step=tf.train.get_or_create_global_step())
txt_encoder_checkpoint.save('./checkpoints2/txt_encoder')
txt_decoder_checkpoint = tf.train.Checkpoint(optimizer=optimizer,
                           model=model.text_decoder,
                           optimizer_step=tf.train.get_or_create_global_step())
txt_decoder_checkpoint.save('./checkpoints2/txt_decoder')


# In[9]:


import numpy as np
def evaluate_image(model, tokenizer, image):
#     attention_plot = np.zeros((max_length, attention_features_shape))

    hidden = model.text_decoder.reset_state(batch_size=1)

    temp_input = tf.expand_dims(data.load_image(image)[0], 0)
    img_tensor_val = model.inception(temp_input)
    img_tensor_val = tf.reshape(img_tensor_val, (img_tensor_val.shape[0], -1, img_tensor_val.shape[3]))

    features = model.image_encoder(img_tensor_val)
    recon = model.image_decoder(features)
    dec_input = tf.expand_dims([tokenizer.word_index['<start>']], 0)
    result = []

    for i in range(49):
        predictions, hidden, attention_weights = model.text_decoder(dec_input, features, hidden)

#         attention_plot[i] = tf.reshape(attention_weights, (-1, )).numpy()

        predicted_id = tf.argmax(predictions[0]).numpy()
        result.append(tokenizer.index_word[predicted_id])

        if tokenizer.index_word[predicted_id] == '<end>':
            return result, recon

        dec_input = tf.expand_dims([predicted_id], 0)

#     attention_plot = attention_plot[:len(result), :]
    return result, recon

def evaluate_image_and_word(model, tokenizer, image, caption):
#     attention_plot = np.zeros((max_length, attention_features_shape))

    hidden = model.text_decoder.reset_state(batch_size=1)

    temp_input = tf.expand_dims(data.load_image(image)[0], 0)
    img_tensor_val = model.inception(temp_input)
    img_tensor_val = tf.reshape(img_tensor_val, (img_tensor_val.shape[0], -1, img_tensor_val.shape[3]))

    features = model.image_encoder(img_tensor_val)
    doc2vec_emb = model.doc2vec.transform(caption)
    print(doc2vec_emb.shape)
    features2 = model.text_encoder(np.array(doc2vec_emb)[np.newaxis, :])
    final_features = tf.reduce_mean([features, features2],axis=0)
    recon = model.image_decoder(final_features)
    dec_input = tf.expand_dims([tokenizer.word_index['<start>']], 0)
    result = []

    for i in range(49):
        predictions, hidden, attention_weights = model.text_decoder(dec_input, features, hidden)

#         attention_plot[i] = tf.reshape(attention_weights, (-1, )).numpy()

        predicted_id = tf.argmax(predictions[0]).numpy()
        result.append(tokenizer.index_word[predicted_id])

        if tokenizer.index_word[predicted_id] == '<end>':
            return result, recon

        dec_input = tf.expand_dims([predicted_id], 0)

#     attention_plot = attention_plot[:len(result), :]
    return result, recon

def evaluate_latent_space(model, tokenizer, image, caption):
#     attention_plot = np.zeros((max_length, attention_features_shape))

    hidden = model.text_decoder.reset_state(batch_size=1)

    temp_input = tf.expand_dims(data.load_image(image)[0], 0)
    img_tensor_val = model.inception(temp_input)
    img_tensor_val = tf.reshape(img_tensor_val, (img_tensor_val.shape[0], -1, img_tensor_val.shape[3]))

    features = model.image_encoder(img_tensor_val)
    doc2vec_emb = model.doc2vec.transform(caption)
#     print(doc2vec_emb.shape)
    features2 = model.text_encoder(np.array(doc2vec_emb)[np.newaxis, :])
    final_features = tf.reduce_mean([features, features2],axis=0)
    
    return features, features2, final_features


# In[9]:


caption = train_captions[29]
img_name = img_name_vector[29]
img_vec = data.load_image(img_name)


# In[10]:


optimizer = tf.train.AdamOptimizer()
img_encoder_checkpoint = tf.train.Checkpoint(optimizer=optimizer,
                           model=model.image_encoder,
                           optimizer_step=tf.train.get_or_create_global_step())
img_encoder_checkpoint.restore(tf.train.latest_checkpoint('./checkpoints4/img_encoder'))
img_decoder_checkpoint = tf.train.Checkpoint(optimizer=optimizer,
                           model=model.image_decoder,
                           optimizer_step=tf.train.get_or_create_global_step())
img_decoder_checkpoint.restore(tf.train.latest_checkpoint('./checkpoints4/img_decoder'))
txt_encoder_checkpoint = tf.train.Checkpoint(optimizer=optimizer,
                           model=model.text_encoder,
                           optimizer_step=tf.train.get_or_create_global_step())
txt_encoder_checkpoint.restore(tf.train.latest_checkpoint('./checkpoints4/txt_encoder'))
txt_decoder_checkpoint = tf.train.Checkpoint(optimizer=optimizer,
                           model=model.text_decoder,
                           optimizer_step=tf.train.get_or_create_global_step())
txt_decoder_checkpoint.restore(tf.train.latest_checkpoint('./checkpoints4/txt_decoder'))


# In[12]:


from matplotlib import pyplot as plt
difs = np.zeros((100, 64, 512))
f1s = np.zeros((100, 64, 512))
f2s = np.zeros((100, 64, 512))
f3s = np.zeros((100, 64, 512))
for i in range(100):
    caption = train_captions[i]
    img_name = img_name_vector[i]
    img_vec = data.load_image(img_name)
    f1, f2, f3 = evaluate_latent_space(model, tokenizer, img_name, caption)
    difs[i,...] = f2-f1
    f3s[i,...] = f3
    f1s[i,...] = f1
    f2s[i,...] = f2
    
difs2 = np.reshape(difs, (100, -1))
f3s2 = np.reshape(f3s, (100, -1))
f1s2 = np.reshape(f1s, (100, -1))

f2s2 = np.reshape(f2s, (100, -1))

for i in range(15):
    j = np.random.randint(0,f2s2.shape[1])
    k = np.random.randint(0,f2s2.shape[1])
    print(j,k)
    plt.figure()
    plt.scatter(f1s2[:, j], f1s2[:,k], c='blue')
    plt.scatter(f2s2[:,j], f2s2[:,k], c='red')
    plt.show()


# In[23]:


from matplotlib import pyplot as plt

difs2 = np.reshape(difs, (100, -1))
f3s2 = np.reshape(f3s, (100, -1))
f1s2 = np.reshape(f1s, (100, -1))

f2s2 = np.reshape(f2s, (100, -1))


# In[24]:


print(np.sum(f2s2/100))


# In[15]:


plt.figure()
plt.scatter(f1s2[:, 1], f1s2[:,2], c='blue')
plt.scatter(f2s2[:,1], f2s2[:,2], c='red')
plt.show()


# In[16]:


from sklearn import manifold
from sklearn import decomposition
# tsne = manifold.TSNE(n_components=2, init='pca', random_state=0)
# Y1 = tsne.fit_transform(f3s2)
pca = decomposition.PCA(n_components=2)
Y1 = pca.fit_transform(f3s2)
plt.scatter(Y1[:, 0], Y1[:, 1],cmap=plt.cm.Spectral)


# In[25]:


caption = train_captions[22]
img_name = img_name_vector[22]
img_vec = data.load_image(img_name)
result, recon = evaluate_image_and_word(model, tokenizer, img_name, caption)


# In[26]:


from matplotlib import pyplot as plt
plt.figure()
plt.imshow((img_vec[0].numpy()))
plt.figure()
plt.imshow((recon.numpy()[0,...]))
print(caption)
print(result)
