FROM prefecthq/prefect:2-python3.10
ADD /. /opt/prefect/flows
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .

