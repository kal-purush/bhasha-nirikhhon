import pandas as pd
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import TfidfVectorizer
from openpyxl import load_workbook
from sklearn import metrics
import pickle


def load_dataset(source):
    col_names = ["sentence", "sentiment"]
    source = pd.read_excel(source, header=None, names=col_names)
    source["sentiment_num"] = source.sentiment.map({"Positive": 1, "Negative": 0})
    return source


def turkish_stopwords():
    # Getting Turkish Stop Words
    with open("stopwords.txt", encoding="utf-8") as stopwords:
        stopwordlist = []
        for word in stopwords:
            stopwordlist.append(word)
    return stopwordlist


def data_from_twitter(source):

    datatest = pd.read_excel(source, header=0)

    X_test = datatest.Tweet

    # Vectorizing our data set
    X_test_dtm = vect.transform(X_test)
    test_predict = nb.predict(X_test_dtm)

    datatest["sentiment_num"] = pd.DataFrame(test_predict, index=datatest.index)
    datatest["sentiment"] = datatest.sentiment_num.map({1: "Positive", 0: "Negative"})

    del(datatest["sentiment_num"])

    data_cluster(datatest, "Tweet")

    with pd.ExcelWriter(source, engine="openpyxl") as writer:
        writer.book = load_workbook(source)
        datatest.to_excel(writer, "Sheet1")
        writer.save()


if __name__ == "__main__":

    data = load_dataset("filmreviews.xlsx")

    X = data.sentence
    y = data.sentiment_num

    # Vectorizing our data set
    vect = TfidfVectorizer(strip_accents="unicode",
                           min_df=7,
                           max_df=0.9,
                           sublinear_tf=True,
                           use_idf=True,
                           stop_words=turkish_stopwords(),
                           ngram_range=(1, 3))

    X_dtm = vect.fit_transform(X)
    # print(vect.get_feature_names())

    # Building and evaluating a model. We will use multinominal Naive Bayes
    nb = MultinomialNB()
    nb.fit(X_dtm, y)

    ex = "LCW'yi çok seviyorum"
    a = pd.Series(ex)
    a = vect.transform(a)
    print(nb.predict_proba(a))

    with open("model.pickle", "wb") as pickle_out:
        pickle.dump(nb, pickle_out)

    with open("vectorizer.pickle", "wb") as pickle_out:
        pickle.dump(vect, pickle_out)