
FROM airbyte/python-connector-base:1.1.0
WORKDIR /airbyte/integration_code
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
ENV AIRBYTE_ENTRYPOINT 'python /airbyte/integration_code/main.py'
ENTRYPOINT ['python', '/airbyte/integration_code/main.py']
