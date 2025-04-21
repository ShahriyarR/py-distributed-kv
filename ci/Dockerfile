FROM python:3.10-buster

WORKDIR /app
RUN apt-get update -y && apt-get install make -y

COPY . .
RUN pip install --upgrade pip
RUN pip install flit==3.8.0
RUN FLIT_ROOT_INSTALL=1 flit install

ENTRYPOINT pytest -s tests
