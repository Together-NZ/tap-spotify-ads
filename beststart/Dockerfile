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
RUN meltano install


# Install any additional Python libraries
RUN pip install     httplib2     google-api-python-client     google-auth     google-auth-oauthlib     google-cloud-bigquery     google-cloud-storage     pandas     pyarrow

# Entrypoint for container
ENTRYPOINT ["meltano"]