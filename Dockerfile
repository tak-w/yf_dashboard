FROM python:3.12-slim

WORKDIR /usr/src/app

COPY /app ./

RUN pip install -r requirements.txt

CMD [ "python", "main.py", "--host=0.0.0.0" ]