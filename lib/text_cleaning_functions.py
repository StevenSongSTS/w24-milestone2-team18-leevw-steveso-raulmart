import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import re

nltk.download("punkt")
nltk.download("stopwords")
nltk.download("wordnet")


def clean_text(text):
    tokens = nltk.word_tokenize(text)   # tokenize but do not lowercase
    stop_words = stopwords.words('english') # remove stopwords
    stop_words.extend([])   # Do not remove financial specific stopwords. They are crucial to the context

    # remove stopwords and special characters
    tokens = [token for token in tokens if token not in stop_words]
    tokens = [re.sub(r"[^\wA-Z']", "", token) for token in tokens]

    # apply lemmatization
    lemmatizer = WordNetLemmatizer()
    tokens = [lemmatizer.lemmatize(token) for token in tokens]

    cleaned_text = ' '.join(tokens)

    return cleaned_text