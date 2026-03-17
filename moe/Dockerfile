# registry.gitlab.com/meltano/meltano:latest is also available in GitLab Registry
ARG MELTANO_IMAGE=meltano/meltano:v3-python3.10
FROM $MELTANO_IMAGE

WORKDIR /project

# Install any additional requirements
COPY ./requirements.txt .
RUN pip install -r requirements.txt
RUN apt-get update && apt-get install -y openssh-client git

# Copy over Meltano project directory
COPY . .
# Install Meltano plugins (will use meltano.yml)
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    ssh -T git@github.com || true && \
    meltano install

RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan github.com >> /root/.ssh/known_hosts && \
    git clone git@github.com:Together-NZ/tap-hivestack.git /tmp/tap-hivestack && \
    git clone git@github.com:Together-NZ/tap-reddit-ads.git /tmp/tap-reddit-ads

RUN pip install httplib2 google-api-python-client google-auth google-auth-oauthlib google-cloud-bigquery pandas db-dtypes

# Ensure plugin venvs have pip + pkg_resources (setuptools)
RUN for v in /project/.meltano/*/*/venv/bin/python; do \
"$v" -m ensurepip --upgrade || true; \
"$v" -m pip install -U "setuptools<81" || true; \
done

# Expose default port used by `meltano ui`
EXPOSE 5000

ENTRYPOINT ["meltano"]
