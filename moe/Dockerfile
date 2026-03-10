# registry.gitlab.com/meltano/meltano:latest is also available in GitLab Registry
ARG MELTANO_IMAGE=meltano/meltano:v3-python3.10
FROM $MELTANO_IMAGE

WORKDIR /project

# Install any additional requirements
COPY ./requirements.txt .
RUN pip install -r requirements.txt


# Copy over Meltano project directory
COPY . .
RUN meltano install 
RUN pip install httplib2 google-api-python-client google-auth google-auth-oauthlib google-cloud-bigquery pandas db-dtypes

# Ensure plugin venvs have pip + pkg_resources (setuptools)
RUN for v in /project/.meltano/*/*/venv/bin/python; do \
"$v" -m ensurepip --upgrade || true; \
"$v" -m pip install -U "setuptools<81" || true; \
done

# Expose default port used by `meltano ui`
EXPOSE 5000

ENTRYPOINT ["meltano"]
