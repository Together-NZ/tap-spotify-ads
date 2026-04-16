# Meltano + dbt-bigquery for squirrel.
#
# Build (private git packages need SSH agent):
#   docker build --ssh default -t meltano-squirrel .
#
# dbt deps: `meltano invoke dbt-bigquery deps` reads transform/packages.yml.

FROM meltano/meltano:v3-python3.10

RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /project

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -U "setuptools<81"

COPY . .

RUN --mount=type=ssh \
    mkdir -p /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts \
    && ssh -T git@github.com || true \
    && meltano install

RUN pip install --no-cache-dir \
    httplib2 \
    google-api-python-client \
    google-auth \
    google-auth-oauthlib \
    google-cloud-bigquery \
    google-cloud-storage \
    pandas \
    pyarrow

RUN for v in /project/.meltano/*/*/venv/bin/python; do \
      "$v" -m ensurepip --upgrade || true; \
      "$v" -m pip install --no-cache-dir -U "setuptools<81" || true; \
    done

RUN --mount=type=ssh \
    mkdir -p /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts \
    && ssh -T git@github.com || true \
    && meltano invoke dbt-bigquery deps

RUN curl -fsSL -o query_standard.json \
      https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_standard.json \
    && curl -fsSL -o query_youtube.json \
      https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_youtube.json

ENTRYPOINT ["meltano"]
