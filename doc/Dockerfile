# Meltano + dbt-bigquery image for doconservation.
#
# Build (private git packages need SSH agent):
#   docker build --ssh default -t meltano-doconservation .
#
# dbt deps: `meltano invoke dbt-bigquery deps` reads transform/packages.yml and
# populates transform/dbt_packages/ inside the image.

FROM meltano/meltano:v3-python3.10

RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /project

# --- Layer cache: pip deps ---
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -U "setuptools<81"

# --- Copy full project (meltano.yml, transform/, taps, etc.) ---
COPY . .

# --- Meltano plugins (extractors, loaders, dbt-bigquery) ---
RUN --mount=type=ssh \
    mkdir -p /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts \
    && ssh -T git@github.com || true \
    && meltano install

# --- Extra Python (BigQuery / APIs) ---
RUN pip install --no-cache-dir \
    httplib2 \
    google-api-python-client \
    google-auth \
    google-auth-oauthlib \
    google-cloud-bigquery \
    google-cloud-storage \
    pandas \
    pyarrow

# --- Plugin venvs: pip + setuptools (dbt / taps) ---
RUN for v in /project/.meltano/*/*/venv/bin/python; do \
      "$v" -m ensurepip --upgrade || true; \
      "$v" -m pip install --no-cache-dir -U "setuptools<81" || true; \
    done

# --- dbt: download packages (git deps → transform/dbt_packages) ---
RUN --mount=type=ssh \
    mkdir -p /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts \
    && ssh -T git@github.com || true \
    && meltano invoke dbt-bigquery deps

# --- DV360 query templates (meltano tap-dv360: query_standard.json / query_youtube.json) ---
RUN curl -fsSL -o dv360_standard.json \
      https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_standard.json \
    && curl -fsSL -o dv360_youtube.json \
      https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_youtube.json

ENTRYPOINT ["meltano"]
