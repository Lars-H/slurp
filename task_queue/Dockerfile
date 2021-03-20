FROM python:2.7-alpine
WORKDIR /task_queue
# layer caching for faster builds
COPY requirements.txt /
RUN pip install -r /requirements.txt

ADD . .
COPY . .
ENTRYPOINT celery -A tasks worker --loglevel=info --concurrency=50 --pool=solo
#-Q celery_worker
#ENTRYPOINT ['celery','-A','test_celery', 'worker', '--loglevel=info']