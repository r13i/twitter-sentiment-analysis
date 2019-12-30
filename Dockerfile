FROM python:3.8-alpine3.11
LABEL maintainer="Redouane Achouri <achouri.a.r@gmail.com>"

RUN apk update && apk add build-base

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY ./app .

# EXPOSE 8000
# HEALTHCHECK CMD xxx

CMD ["python", "app.py"]