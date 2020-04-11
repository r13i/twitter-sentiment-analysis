FROM python:3.8-alpine3.11
LABEL maintainer="Redouane Achouri <redouane.a.achouri@gmail.com>"

RUN apk update && apk add build-base

WORKDIR /consume-tweets
COPY . .

# Download data and modules required by the NLTK library
# for pre-processing tweets
RUN pip install -r requirements.txt && \
    python -m nltk.downloader twitter_samples && \
    python -m nltk.downloader punkt && \
    python -m nltk.downloader wordnet && \
    python -m nltk.downloader averaged_perceptron_tagger && \
    python -m nltk.downloader stopwords

# EXPOSE xxxx
# HEALTHCHECK CMD xxx

CMD ["python", "run.py"]