FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/

RUN pip3 install -r ./requirements.txt

COPY . /app

RUN chmod +x ./entrypoint.sh

CMD ["python3", "main.py"]