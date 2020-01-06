FROM python:3.6-alpine3.11
LABEL maintainer="Redouane Achouri <redouane.a.achouri@gmail.com>"

RUN apk update && apk add build-base

WORKDIR /consume-tweets
COPY . .

RUN pip install -r requirements.txt

# EXPOSE xxxx
# HEALTHCHECK CMD xxx

CMD ["python", "run.py"]