# Base Meltano image
FROM meltano/meltano:latest

# Enable SSH agent forwarding
# Required for cloning private GitHub repos via SSH
# Must be enabled via `--ssh default` during build
RUN apt-get update && apt-get install -y openssh-client git curl

# Set working directory
WORKDIR /project

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy project files
COPY . .

# Install Meltano plugins (will use meltano.yml)
# Install Meltano plugins (will use meltano.yml)
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano install
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano invoke dbt-bigquery deps
# Install any additional Python libraries
RUN pip install     httplib2     google-api-python-client     google-auth     google-auth-oauthlib     google-cloud-bigquery     google-cloud-storage     pandas     pyarrow

# Ensure plugin venvs have pip + pkg_resources (setuptools)
RUN for v in /project/.meltano/*/*/venv/bin/python; do \
"$v" -m ensurepip --upgrade || true; \
"$v" -m pip install -U "setuptools<81" || true; \
done
RUN curl -sSL -o dv360_standard.json https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_standard.json
RUN curl -sSL -o dv360_youtube.json https://raw.githubusercontent.com/Together-NZ/Impact-CI-DV360_models/main/dv360_youtube.json
# Entrypoint for container
ENTRYPOINT ["meltano"]
