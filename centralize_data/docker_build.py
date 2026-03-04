import argparse
import os
import subprocess

from client_name import CLIENT_NAME



class DockerBuild:
    def __init__(self, project_name):
        self.project_name = project_name
        self.image = f"australia-southeast1-docker.pkg.dev/{project_name}-main/meltano/meltano-{project_name}-main:stage"

    def __return_image(self):
        return self.image

    def build_command(self):
        
        return [
            "docker", "buildx", "build",
            "--ssh", "default",
            "--platform", "linux/amd64",
            "-t", self.__return_image(),
            "."
        ]
        
    def test_command(self):
        return [
            "docker", "run", "--rm", "--entrypoint", "sh", self.__return_image(),
            "ci_test.sh"
        ]

    def push_command(self):
        return [
            "docker", "push", self.__return_image()
        ]

# Environment variable for enabling BuildKit
env = {**os.environ, "DOCKER_BUILDKIT": "1"}

def run_cmd(cmd):
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def run_ci_test_sh(cmd):

    run_cmd(cmd)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build, test, and push Docker image")
    parser.add_argument("project_name",choices=CLIENT_NAME.keys())
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--build-only", action="store_true", help="Only build the image")
    group.add_argument("--push-only", action="store_true", help="Only push the image (assumes image already built)")
    group.add_argument("--test-only", action="store_true", help="Only run ci_test.sh inside the container")
    
    args = parser.parse_args()

    gcp_name = CLIENT_NAME[args.project_name]


    if args.build_only:
        print("🚀 Building Docker image...")
        build_command = DockerBuild(args.project_name).build_command()
        run_cmd(build_command)
        print("✅ Build done.")
    elif args.push_only:
        print("📦 Pushing Docker image...")
        push_command = DockerBuild(args.project_name).push_command()
        run_cmd(push_command)
        print("✅ Push done.")
    elif args.test_only:
        test_command = DockerBuild(args.project_name).test_command()
        print("🧪 Running ci_test.sh inside container...")
        run_cmd(test_command)
        print("✅ Test done.")
    else:
        raise ValueError("Invalid argument")
