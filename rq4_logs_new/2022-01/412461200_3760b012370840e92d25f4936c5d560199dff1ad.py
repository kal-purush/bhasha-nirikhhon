import streamlit as st
import numpy as np
from sklearn import datasets


st.title("Test")

st.write("""
# Test different classifiers
Choose different sets.
""")


dataset_name = st.sidebar.selectbox("Choose DataSet", ("Iris", "Breast Cancer", "Wine"))
classifier_name = st.sidebar.selectbox("Choose Classifier", ("KNN", "SVM", "Random Forest"))


def get_dataset(dataset_name):
    if dataset_name == "Iris":
        data = datasets.load_iris()
    elif dataset_name == "Breast Cancer":
        data = datasets.load_breast_cancer()
    else:
        data = datasets.load_wine()
    X = data.data
    Y = data.target
    return X, Y

X, Y = get_dataset(dataset_name)
st.write("Dataset shape", X.shape)
st.write("number of classes", len(np.unique(Y)))