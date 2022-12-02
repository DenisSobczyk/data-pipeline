FROM python:3.6

USER root

WORKDIR /app

ADD ./requirements.txt /app

RUN pip install --trusted-host pypi.python.org -r requirements.txt

EXPOSE 8050

ENV NAME World

CMD ["python", "/opt/dash/data/dash_app_1201.py"]