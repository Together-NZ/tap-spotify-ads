import argparse
import os
import subprocess

IMAGE = "australia-southeast1-docker.pkg.dev/amp-main/meltano/meltano-amp-main:prod"

# Path to ci_test.sh (same folder as this script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CI_TEST_SH = os.path.join(SCRIPT_DIR, "ci_test.sh")

# Build the Docker image using BuildKit with SSH forwarding
build_command = [
    "docker", "buildx", "build",
    "--ssh", "default",
    "--platform", "linux/amd64",
    "-t", IMAGE,
    "."
]

test_command = [
    "docker", "run", "--rm", "--entrypoint", "sh", IMAGE,
    "ci_test.sh"
]

push_command = [
    "docker", "push", IMAGE
]

# Environment variable for enabling BuildKit
env = {**os.environ, "DOCKER_BUILDKIT": "1"}

def run_cmd(cmd):
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def run_ci_test_sh(on_host=False):
    """Run ci_test.sh from the same folder. Use on_host=True to run locally, False to run inside the built image."""
    if on_host:
        subprocess.run(["sh", CI_TEST_SH], check=True, cwd=SCRIPT_DIR, env=env)
    else:
        run_cmd(test_command)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build, test, and push Docker image")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--build-only", action="store_true", help="Only build the image")
    group.add_argument("--push-only", action="store_true", help="Only push the image (assumes image already built)")
    group.add_argument("--test-only", action="store_true", help="Only run ci_test.sh inside the container")
    args = parser.parse_args()

    if args.build_only:
        print("🚀 Building Docker image...")
        run_cmd(build_command)
        print("✅ Build done.")
    elif args.push_only:
        print("📦 Pushing Docker image...")
        run_cmd(push_command)
        print("✅ Push done.")
    elif args.test_only:
        print("🧪 Running ci_test.sh inside container...")
        run_ci_test_sh(on_host=False)
        print("✅ Test done.")
    else:
        print("🚀 Building Docker image...")
        run_cmd(build_command)
        print("🧪 Running ci_test.sh inside container...")
        run_ci_test_sh(on_host=False)
        print("📦 Pushing Docker image...")
        run_cmd(push_command)
        print("✅ Done.")
