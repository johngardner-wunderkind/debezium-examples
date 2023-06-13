FROM python:3.8-slim

COPY requirements.txt .
COPY raw_events-key.avsc .
COPY raw_events-value.avsc .

RUN set -ex; \
	pip install --no-cache-dir -r requirements.txt

ADD raw_events.py .

CMD ["python", "-u", "./raw_events.py"]
