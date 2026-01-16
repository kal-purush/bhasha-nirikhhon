from pyspark import SparkContext
from thunder.utils.context import ThunderContext
from thunder import Colorize

sc = SparkContext(appName="Start")
tsc = ThunderContext(sc)
    
data=tsc.loadImages('file:///Users/tarunjoshi/softwares/101_ObjectCategories/Faces/',inputFormat='png')
type(data)
#data.nrecords
#Image  = Colorize.image
data.saveAsBinaryImages('/Users/tarunjoshi/softwares/spark-1.4.1-bin-hadoop2.6/apps/my-binary-directory/', overwrite=True)
#tsc.export(img,"directory","npy")