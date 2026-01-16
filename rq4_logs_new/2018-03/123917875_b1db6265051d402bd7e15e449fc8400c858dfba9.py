import sys
import os.path

from src.extract.Extract import Extract
from src.transform.Transform import Transform
from src.context.JobContext import JobContext

dictionary_path = os.path.dirname(__file__)+'/resources/dictionary.txt'
events_path = os.path.dirname(__file__)+'/resources/events.csv'
game_info_path = os.path.dirname(__file__)+'/resources/ginf.csv'

def test_ShotAnalysis():
  try:
    rdd_extract = Extract(dictionary_path,events_path,game_info_path).extract()
    print(rdd_extract.collect())
  finally:
    JobContext.stop()