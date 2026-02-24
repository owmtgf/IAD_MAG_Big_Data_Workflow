FROM python:3.10

WORKDIR /app
COPY ./consumers/ml_classifier/requirements.txt .
RUN pip install -r requirements.txt

COPY ./consumers/ml_classifier/app.py .
COPY ./classifier_preparation/model ./model

ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]