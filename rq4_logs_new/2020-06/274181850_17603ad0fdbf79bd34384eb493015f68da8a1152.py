from utils import *
import math

crime = open("../data/Crime.txt", "r")
crimeString= crime.read()
crimeString=extract_characters(crimeString)
freqCrime=freq_array(crimeString)
pairProbsCrime=get_pair_freq(crimeString)

probabilitiesCrime=list(freqCrime)
for i in range (len(probabilitiesCrime)):
    probabilitiesCrime[i]=probabilitiesCrime[i]/sum(freqCrime)

def test_liklihood_difference():
    text="hi my name is bethany and every single letter in this sentence is perfectly correct"
    text2="hd my name ds bethany ani every sdngle letter dn thds sentence ds perfectly correct"
    diffFun=calc_liklihood_difference(3,8,get_pairs_array(text2),probabilitiesCrime, pairProbsCrime)
    diff=log_liklihood_pairs(text, probabilitiesCrime, pairProbsCrime)-log_liklihood_pairs(text2, probabilitiesCrime, pairProbsCrime)
    assert math.isclose(diff,diffFun), "should be equal"
    
test_liklihood_difference()

print('test complete')