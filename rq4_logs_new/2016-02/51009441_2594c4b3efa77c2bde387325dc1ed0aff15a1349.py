import click
import zipfile
import base64
from merkle import *

class LeafGenerator:
  def createLeafInput(payload, timestamp):
    bytearray = b'\x00\x00'
    
    bytearray += timestamp.to_bytes(8, byteorder='big')

    bytearray += b'\x80\x00'
    
    bytearray += len(payload).to_bytes(3, byteorder='big')
    
    bytearray += payload.encode()

    bytearray += b'\x00\x00'
    
    return base64.b64encode(bytearray)


# @click.command()
# @click.option('--filename', prompt='downloaded file name')
# def run(filename):
def run():
  # result = zipfile.is_zipfile(filename)
  # print(result)
  # print("finename provided is: " + filename)
  # leaf = LeafGenerator("AAAAAAFSeasJ5IAAAABZeyAib3duZXIiOiAiRm9yZXN0cnkgQ29tbWlzc2lvbiIsICJlbmQtZGF0ZSI6ICIiLCAiZ292ZXJubWVudC1kb21haW4iOiAiN3N0YW5lcy5nb3YudWsiIH0AAA==")
  # leaf = LeafGenerator.createLeafInput('{"key":"value"}', 1452868835667)
  # print(leaf)

 

  merkle_tree = MerkleTree()
  

  # encodedValue = "AAAAAAFSjeCvzYAAAAAUeyAidGhpbmciOiAic3R1ZmYiIH0AAA==".encode('utf-8')
  # print(encodedValue)
  merkle_tree.add(b'\x00' + '{ "thing": "stuff" }'.encode('utf-8'))
  
  merkle_tree.add(b'\x00' + '{ "huh, no cbor? :)": "nope, none!" }'.encode('utf-8'))
  
  print(base64.b64encode(merkle_tree.build()))

if __name__ == '__main__':
  run()



