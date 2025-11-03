import subprocess

# Build the Docker image using BuildKit with SSH forwarding
build_command = [
    "docker", "buildx", "build",
    "--ssh", "default",
    "--platform", "linux/amd64",
    "-t", "australia-southeast1-docker.pkg.dev/cffc-main/meltano/meltano-cffc-main:prod",
    "."
]

# Push the image to the remote registry
push_command = [
    "docker", "push",
    "australia-southeast1-docker.pkg.dev/cffc-main/meltano/meltano-cffc-main:prod"
]

# Environment variable for enabling BuildKit
env = {**subprocess.os.environ, "DOCKER_BUILDKIT": "1"}

def run_cmd(cmd):
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")

if __name__ == "__main__":
    print("🚀 Building Docker image...")
    run_cmd(build_command)
    
    print("📦 Pushing Docker image...")
    run_cmd(push_command)

    print("✅ Done.")
