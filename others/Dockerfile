FROM python:3.11.15-slim

WORKDIR /setup

COPY . .

RUN pip install -r ./requirements.txt

CMD [ "python", "./main.py" ]