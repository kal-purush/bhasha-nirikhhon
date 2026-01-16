'''
based on the paper: Statistical Significance Tests for Machine Translation Evaluation
Philipp Koehn
https://www.aclweb.org/anthology/W04-3250.pdf

steps for boostrap resampling on one dataset with one model:
1. get all the predictions on fnc-dev (9068 data points) by a given model (say you pick a model which has accuracy of 74%)
2. from this randomly pick 300 data points
for 1 to 1000:
    3. from this 300 pick 300 data points, with replacement
    4. calculate accuracy, note it down
    5. repeat 1000 times

6. plot all these accuracies
7. check if 95% of the time you get an accuracy of 74% or more




steps for boostrap resampling on a pair of models:


'''