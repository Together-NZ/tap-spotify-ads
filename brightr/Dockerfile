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
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    git clone git@github.com:Together-NZ/tap-hivestack.git /tmp/tap-hivestack

# Copy project files
COPY . .

# Install Meltano plugins (will use meltano.yml)
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    bash -c '\
        function retry() { for i in {1..3}; do "$@" && break || sleep 5; done; } && \
        retry meltano install extractor tap-ttd && \
        retry meltano install extractor tap-facebook && \
        retry meltano install extractor tap-ga4 && \
        retry meltano install extractor tap-hivestack && \
        retry meltano install loader target-bigquery && \
        retry meltano install transformer dbt-bigquery\
    '


# Install any additional Python libraries
RUN pip install \
    httplib2 \
    google-api-python-client \
    google-auth \
    google-auth-oauthlib \
    google-cloud-bigquery \
    google-cloud-storage \
    pandas \
    pyarrow

# Ensure plugin venvs have pip + pkg_resources (setuptools)
RUN for v in /project/.meltano/*/*/venv/bin/python; do \
"$v" -m ensurepip --upgrade || true; \
"$v" -m pip install -U "setuptools<81" || true; \
done

# Entrypoint for container
ENTRYPOINT ["meltano"]

