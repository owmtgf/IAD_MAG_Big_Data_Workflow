FROM python:3.10

WORKDIR /app
COPY ./consumers/preprocessor/requirements.txt .
RUN pip install -r requirements.txt

COPY ./consumers/preprocessor/app.py .
COPY ./classifier_preparation/preprocessing_scripts.py .
COPY ./classifier_preparation/model ./model

ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]