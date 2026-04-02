FROM meltano/meltano:v3-python3.10

RUN apt-get update && apt-get install -y openssh-client git curl

WORKDIR /project

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install -U "setuptools<81"

RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    git clone git@github.com:Together-NZ/tap-pinterest-ads.git /tmp/tap-pinterest-ads

COPY . .

RUN curl -sSL -o dv360_standard.json https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_standard.json
RUN curl -sSL -o dv360_youtube.json https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_youtube.json

RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    bash -c '\
        function retry() { for i in {1..3}; do "$@" && break || sleep 5; done; } && \
        retry meltano install extractor tap-ga4 && \
        retry meltano install extractor tap-facebook && \
        retry meltano install extractor tap-ttd && \
        retry meltano install extractor tap-dv360 && \
        retry meltano install extractor tap-pinterest-ads && \
        retry meltano install loader target-bigquery && \
        retry meltano install loader target-jsonl && \
        retry meltano install transformer dbt-bigquery \
    '

RUN pip install \
    httplib2 \
    google-api-python-client \
    google-auth \
    google-auth-oauthlib \
    google-cloud-bigquery \
    google-cloud-storage \
    pandas \
    pyarrow

RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano invoke dbt-bigquery deps

ENTRYPOINT ["meltano"]
