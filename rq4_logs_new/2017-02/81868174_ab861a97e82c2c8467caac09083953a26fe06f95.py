import os
import matplotlib.pyplot as plt
plt.ion()

class SSLearnPipeline(object):
  def __init__(self, outputdir, output_prefix, max_boxes_in_one_image, total_to_label=250):
    self.outputdir = outputdir
    assert os.path.exists(outputdir)
    self.output_prefix = output_prefix
    self.labeled_dir = os.path.join(outputdir, 'labeled')
    if not os.path.exists(self.labeled_dir):
      os.mkdir(self.labeled_dir)
    self.total_to_label = total_to_label
    self.max_boxes_in_one_image = max_boxes_in_one_image

  def get_category(self, boxes_labeled):
    assert len(boxes_labeled) <= self.max_boxes_in_one_image
    assert len(boxes_labeled) >= 0
    if boxes_labeled == []: return 0
    if boxes_labeled == [1]: return 1
    if boxes_labeled == [2]: return 2
    if boxes_labeled == [3]: return 3
    raise Exception("not fully implemented")

  def image_already_labeled(self, keystr):
    return False

  def labeling_not_done(self):
    return True

  def label(self, img, keystr):
    def not_valid(ans):
      ans = ans.lower()
      if ans == 'no':
        return False
      try:
        d = int(ans)
      except:
        return True
      return False
      
    if self.image_already_labeled(keystr):
      return None

    plt.figure(1)
    plt.imshow(img)
    plt.show()
    plt.pause(.1)
    ans = ''
    while not_valid(ans):
      ans = raw_input("Enter the number of boxes to label, or no to skip this image: ") 
      if not_valid(ans):
        print("--ERROR: input %s not understood" % ans)
    if ans == 'no':
      return
    num_boxes_to_label = int(ans)
#    category = self.get_category(num_boxes_to_label)
    # decide on the output name
    # construct the command line to run labelme
    # look at the output afterwards
    

  
  
  

  
  