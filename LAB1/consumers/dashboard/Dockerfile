FROM python:3.10

WORKDIR /app

COPY consumers/dashboard/requirements.txt .
RUN pip install -r requirements.txt

COPY consumers/dashboard/app.py .

ENV PYTHONUNBUFFERED=1

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]