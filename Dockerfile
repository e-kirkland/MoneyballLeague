FROM python:3.7-slim
ENV PYTHONUNBUFFERED 1
ENV PYTHONIOENCODING UTF-8
WORKDIR /app
ADD . /app
COPY ./ ./
RUN apt-get -y update
RUN apt-get install -y gcc python3-dev curl
RUN pip install -r requirements.txt
RUN pip uninstall slackclient
RUN pip install slackclient
EXPOSE 5000
# RUN api app
CMD ["python", "-u", "app.py"]