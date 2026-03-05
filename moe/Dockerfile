FROM meltano/meltano:v3-python3.10

WORKDIR /project

# Install any additional requirements
COPY ./requirements.txt .
RUN pip install -r requirements.txt


# Copy over Meltano project directory
COPY . .
RUN meltano install 
RUN pip install httplib2 google-api-python-client google-auth google-auth-oauthlib google-cloud-bigquery pandas db-dtypes


# Expose default port used by `meltano ui`
EXPOSE 5000

ENTRYPOINT ["meltano"]
