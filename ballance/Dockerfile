# Base Meltano image
FROM meltano/meltano:latest

# Enable SSH agent forwarding
# Required for cloning private GitHub repos via SSH
# Must be enabled via `--ssh default` during build
RUN apt-get update && apt-get install -y openssh-client git

# Set working directory
WORKDIR /project

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install -r requirements.txt
# Mount SSH and trust GitHub SSH host key
# Mount SSH and trust GitHub host key

# Copy project files
COPY . .

# Install Meltano plugins (will use meltano.yml)
RUN bash -c '\
  for i in {1..3}; do meltano install extractor tap-ttd        && break || sleep 5; done && \
  for i in {1..3}; do meltano install extractor tap-dv360      && break || sleep 5; done && \
  for i in {1..3}; do meltano install extractor tap-ga4        && break || sleep 5; done && \
  for i in {1..3}; do meltano install extractor tap-facebook   && break || sleep 5; done && \
  for i in {1..3}; do meltano install loader target-bigquery   && break || sleep 5; done && \
  for i in {1..3}; do meltano install orchestrator airflow     && break || sleep 5; done && \
  for i in {1..3}; do meltano install transformer dbt-bigquery && break || sleep 5; done && \
  for i in {1..3}; do meltano install file files-airflow       && break || sleep 5; done'



# Install any additional Python libraries
RUN pip install     httplib2     google-api-python-client     google-auth     google-auth-oauthlib     google-cloud-bigquery     google-cloud-storage     pandas     pyarrow
# Ensure plugin venvs have pip + pkg_resources (setuptools)
RUN for v in /project/.meltano/*/*/venv/bin/python; do \
"$v" -m ensurepip --upgrade || true; \
"$v" -m pip install -U "setuptools<81" || true; \
done
# Entrypoint for container
ENTRYPOINT ["meltano"]