FROM python:3.8
# WORKDIR /Docker2/emddocker
RUN apt-get update
ADD emsdocker/ems_new.py ems_new.py

# install dependencies that are required.

RUN pip install paho-mqtt
RUN pip install influxdb_client
RUN pip install pandas
CMD ["python3","./ems_new.py"]
