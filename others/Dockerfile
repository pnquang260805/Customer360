FROM python:3.11.15-slim

WORKDIR /setup

COPY . .
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN chmod +x ./setup.sh

RUN pip install -r ./requirements.txt

CMD [ "./setup.sh" ]