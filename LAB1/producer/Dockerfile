FROM python:3.10

WORKDIR /app

COPY ./producer/requirements.txt .
RUN pip install -r requirements.txt

COPY producer/app.py .

COPY classifier_preparation/dataset_chunked_raw /app/data_source

ENV PYTHONUNBUFFERED=1

CMD ["python", "app.py"]