seed = tf.random.normal([5, 100])
for i in range(5000):
  for image_batch in train_dataset1:
    train_step(image_batch,BATCH_SIZE)
    if i%25 == 0:
      print(i)
      generate_and_save_images(seed)    

for i in range(5000):
  for image_batch in train_dataset2:
    train_step(image_batch,BATCH_SIZE)
    if i%25 == 0:
      print(i+5000)
      generate_and_save_images(seed)  

for i in range(5000):
  for image_batch in train_dataset3:
    train_step(image_batch,BATCH_SIZE)
    if i%25 == 0:
      print(i+10000)
      generate_and_save_images(seed)  

for i in range(5000):
  for image_batch in train_dataset4:
    train_step(image_batch,BATCH_SIZE)
    if i%25 == 0:
      print(i+15000)
      generate_and_save_images(seed)  

for i in range(5000):
  for image_batch in train_dataset5:
    train_step(image_batch,BATCH_SIZE)
    if i%25 == 0:
      print(i+20000)
      generate_and_save_images(seed)  