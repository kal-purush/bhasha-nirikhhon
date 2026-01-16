from .train import tokenizer, model, preprocessing, json, np, LogisticRegression, tf, torch

def load_model():
    with open('/Users/percyd/Docs/Code/mentalist/mentalist/mentalist_app/nlp_model/model.json', 'r') as file:
        dict_ = json.load(file)
    new_model = LogisticRegression()
    new_model.coef_ = np.array(dict_['coef'])
    new_model.intercept_ = np.array(dict_['intercept'])
    new_model.classes_ = np.array(dict_['classes'])
    new_model.n_features_in_ = dict_['n_features_in']
    new_model.n_iter_ = dict_['n_iter']
    return new_model

def tokenize_text(text):
    tokenized_ans = tokenizer.encode(text, add_special_tokens=True)
    padded_test = tf.keras.preprocessing.sequence.pad_sequences([tokenized_ans], len(tokenized_ans), padding='post')
    attention_mask = np.where(padded_test!=0, 1, 0)
    input_ids = torch.tensor(padded_test)
    attention_mask = torch.tensor(attention_mask)
    with torch.no_grad():
        last_hidden_states = model(input_ids.long(), attention_mask=attention_mask)
    return last_hidden_states[0][:, 0, :].numpy()
    

def main():
    print("How are you feel today?")
    ans = input()
    tokenized_ans = tokenizer.encode(ans, add_special_tokens=True)
    padded_test = tf.keras.preprocessing.sequence.pad_sequences([tokenized_ans], len(tokenized_ans), padding='post')
    attention_mask = np.where(padded_test!=0, 1, 0)
    input_ids = torch.tensor(padded_test)
    attention_mask = torch.tensor(attention_mask)
    with torch.no_grad():
        last_hidden_states = model(input_ids.long(), attention_mask=attention_mask)
    ans_feature = last_hidden_states[0][:, 0, :].numpy()
    lr_clf = load_model()
    prediction = lr_clf.predict(ans_feature)[0]
    print("Your mental issue is: ", prediction)

if __name__ == "__main__":
    main()