# Des biblioth�ques qu'il faut installer

#pip install nltk
#pip install python-twitter
#vous tapez dans le console la commande : import nltk puis nltk.download() . Apr�s une fenetre va apparaitre puis installer tous les packages qui te donne.


import twitter
import re
import nltk
from nltk.tokenize import word_tokenize
from string import punctuation
from nltk.corpus import stopwords

# Phase d'initialisation
twitter_api = twitter.Api(consumer_key='z6W3ZLobKsJmPePLOyzLT1eCf',
                        consumer_secret='Tp5o5NpZVkGoXWjIa9qQnbuYdjHymL5GMb73O100Yla4wGjkpx',
                        access_token_key='875363267048349697-cVuMnLdzBHGbzuWJclc1BVe3pFspKfk',
                        access_token_secret='0wJIesEYxucO1xMFzxFGX9kAvK8Cmkf0mqYRnSfkfPkaN')

print(twitter_api.VerifyCredentials())


def Construction_Du_TestSet(mot_cl�):
    try:
        Tweet_r�cup�r� = twitter_api.GetSearch(mot_cl�, count=100)

        print( str(len(Tweet_r�cup�r�)) + " Tweets ont �t� r�cup�r� contenant le mot :" + mot_cl�)

        return [{"text": Status_Du_Tweet.text, "label": None} for Status_Du_Tweet in Tweet_r�cup�r�]
    except:
        print("ERREUR !!")
        return None

mot_cl� = input("Saisir le mot cl� ")
Data_To_Use_For_Testing = Construction_Du_TestSet(mot_cl�)



def Construire_Le_Training_Set(FichierCorpus, FichierTweets):
    import csv
    import time

    L = []
    with open(FichierCorpus, 'r') as csvfile:
        lecture_ligne = csv.reader(csvfile, delimiter=',')
        for row in lecture_ligne:
            try:
                status = twitter_api.GetStatus(row[2]) # on importe le tweet correspond au tweet_id (row[2] =tweet_id)
                print("Tweet fetched" + status.text)    # on extrait la partie texte du tweet qu'on vient d'importer .
                L.append({"tweet_id": row[2],"text":status.text, "label": row[1], "topic": row[0]}) # Enfin de l'execution on formera une liste contenat le training set.
                time.sleep(900 / 180)
            except:
                continue

    with open(FichierTweets, 'w') as csvfile:
        Ecriture_Ligne = csv.writer(csvfile, delimiter=',')
        for tweet in L:
            try:
                Ecriture_Ligne.writerow([tweet["tweet_id"], tweet["text"], tweet["label"], tweet["topic"]])
            except Exception as e:
                print(e)
    return L

FichierCorpus = "C:/Users/hp/Desktop/corpus.csv"
FichierTweets = "C:/Users/hp/Desktop/tweetDataFile.csv"
Data_To_Use_For_Training  = Construire_Le_Training_Set(FichierCorpus, FichierTweets)



stopwords = set(stopwords.words('english') + list(punctuation) + ['AT_USER', 'URL'])

def pr�traitement_Du_Tweets(Data_to_process):
    Tweets_Pr�trait�s = []
    for tweet in Data_to_process:
        tweet["text"] = tweet["text"].lower()
        tweet["text"] = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet["text"])
        tweet["text"] = re.sub('@[^\s]+', 'AT_USER', tweet["text"])
        tweet["text"] = re.sub(r'#([^\s]+)', r'\1', tweet["text"])
        tweet["text"] = word_tokenize(tweet["text"])
        a=[mot for mot in tweet["text"] if mot not in stopwords]
        Tweets_Pr�trait�s.append((a,tweet["label"]))
    return Tweets_Pr�trait�s

TrainingSet_Pr�trait� = pr�traitement_Du_Tweets(Data_To_Use_For_Training)
TestSet_Pr�trait� = pr�traitement_Du_Tweets(Data_To_Use_For_Testing)


def Liste_Des_Mots(TrainingData_Pr�trait�):
    Ensemble_Des_Mots = []
    for (mots, sentiment) in TrainingData_Pr�trait�:
        Ensemble_Des_Mots.extend(mots)
    DesMots_Avec_Leur_Fr�quence = nltk.FreqDist(Ensemble_Des_Mots).keys()
    return DesMots_Avec_Leur_Fr�quence

def Tirer_Des_Caract(tweet):
    appartenance_des_mots={}
    for mot in Caract_des_mots:
        appartenance_des_mots['Est ce qu\' il contient le mot (%s) ?' % mot] = (mot in set(tweet))
    return appartenance_des_mots

Caract_des_mots = Liste_Des_Mots(TrainingSet_Pr�trait�)
Caract_de_TrainingSet = nltk.classify.apply_features(Tirer_Des_Caract, TrainingSet_Pr�trait�)


NBayesClassifier = nltk.NaiveBayesClassifier.train(Caract_de_TrainingSet)

NBResultLabels = [NBayesClassifier.classify(Tirer_Des_Caract(tweet[0])) for tweet in TestSet_Pr�trait� ]

# Maintenant vous aurez la nature du sentiment est qu'il est positive ou pas
if NBResultLabels.count('positive') > NBResultLabels.count('negative'):
    print("C'est un sentiment positive avec le pourcentage  " + str(100*NBResultLabels.count('positive')/len(NBResultLabels)) + "%")
else:
    print("C'est un sentiment n�gative avec le pourcentage" + str(100*NBResultLabels.count('negative')/len(NBResultLabels)) + "%")