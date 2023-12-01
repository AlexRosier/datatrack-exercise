FROM python:3


ADD requirements.in requirements.in

RUN pip install -r requirements.in
#RUN pip-compile requirements.in -v --resolver=legacy

ADD src/integrated_exercise/ingest/datatrack_orchestrator.py datatrack_orchestrator.py
ADD src/integrated_exercise/ingest/api_reader.py api_reader.py
ADD src/integrated_exercise/ingest/s3_writer.py s3_writer.py
ADD src/integrated_exercise/ingest/station_smasher.py station_smasher.py
ADD src/integrated_exercise/ingest/timeseries_smasher.py timeseries_smasher.py
ADD src/integrated_exercise/ingest/timeseries_data_smasher.py timeseries_data_smasher.py