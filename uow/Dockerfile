FROM meltano/meltano:v3-python3.10

WORKDIR /project

# Copy dependencies early to cache layers
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy project files
COPY . .

# Retry-based installation per extractor (prevents one flaky plugin from failing all)
RUN bash -c 'for i in {1..3}; do meltano install extractor tap-ttd && break || sleep 5; done'
RUN bash -c 'for i in {1..3}; do meltano install extractor tap-dv360 && break || sleep 5; done'
RUN bash -c 'for i in {1..3}; do meltano install extractor tap-tiktok && break || sleep 5; done'
RUN bash -c 'for i in {1..3}; do meltano install extractor tap-snapchat-ads && break || sleep 5; done'
RUN bash -c 'for i in {1..3}; do meltano install extractor tap-ga4 && break || sleep 5; done'
RUN bash -c 'for i in {1..3}; do meltano install extractor tap-facebook && break || sleep 5; done'

RUN bash -c 'for i in {1..3}; do meltano install loader target-bigquery && break || sleep 5; done'
RUN bash -c 'for i in {1..3}; do meltano install transformer dbt-bigquery && break || sleep 5; done'

# Optional: if needed
#RUN bash -c 'for i in {1..3}; do meltano install utility run_python && break || sleep 5; done'

# Optional: Additional Python packages
RUN pip install httplib2 google-api-python-client google-auth google-auth-oauthlib google-cloud-bigquery pandas db-dtypes pyhumps

RUN pip install --no-cache-dir "pyhumps>=3.8.0"
EXPOSE 5000 
ENTRYPOINT ["meltano"]
