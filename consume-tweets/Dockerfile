FROM python:3.8-alpine3.11
LABEL maintainer="Redouane Achouri <redouane.a.achouri@gmail.com>"

RUN apk update && apk add build-base

WORKDIR /consume-tweets
COPY . .

RUN pip install -r requirements.txt

# RUN python -m nltk.downloader twitter_samples && \
#     python -m nltk.downloader punkt

# EXPOSE xxxx
# HEALTHCHECK CMD xxx

CMD ["python", "run.py"]