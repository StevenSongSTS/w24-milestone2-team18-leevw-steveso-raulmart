import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

nltk.download("punkt")
nltk.download("stopwords")
nltk.download("wordnet")


def clean_text(text):
    tokens = nltk.word_tokenize(text.lower())  # lowercase text
    stop_words = stopwords.words("english")  # remove stopwords
    stop_words.extend([])  # TO DO: find a list of financial specific stopwords online

    # tokenize and remove special characters like punctuation
    tokens = [token for token in tokens if token not in stop_words]
    tokens = [token for token in tokens if token.isalnum()]

    # apply lemmatization
    lemmatizer = WordNetLemmatizer()
    tokens = [lemmatizer.lemmatize(token) for token in tokens]

    cleaned_text = " ".join(tokens)

    return cleaned_text
