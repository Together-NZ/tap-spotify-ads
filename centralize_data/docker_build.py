import argparse
import os
import subprocess

from client_name import CLIENT_NAME



class DockerBuild:
    def __init__(self, project_name, tag="stage"):
        gcp_name = CLIENT_NAME[project_name]
        if gcp_name == "together-internal":
            self.image = f"australia-southeast1-docker.pkg.dev/{gcp_name}/meltano/meltano-{gcp_name}:{tag}"
        else:
            self.image = f"australia-southeast1-docker.pkg.dev/{gcp_name}-main/meltano/meltano-{gcp_name}-main:{tag}"

    def build_command(self):
        return [
            "docker", "buildx", "build",
            "--ssh", "default",
            "--platform", "linux/amd64",
            "-t", self.image,
            "."
        ]

    def test_command(self, ci_test_path="ci_test.sh"):
        return [
            "docker", "run", "--rm",
            "-v", f"{os.path.abspath(ci_test_path)}:/project/ci_test.sh",
            "--entrypoint", "sh", self.image,
            "/project/ci_test.sh"
        ]
    def retag_command(self, tag):
        new_image = self.image.rsplit(":", 1)[0] + f":{tag}"
        return [
            "docker", "tag", self.image, new_image
        ]
    def push_command(self, tag):
        new_image = self.image.rsplit(":", 1)[0] + f":{tag}"
        return [
            "docker", "push", new_image
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
    parser.add_argument("project_name", choices=CLIENT_NAME.keys())
    parser.add_argument("--ci-test-path", default="../centralize_data/ci_test.sh", help="Path to ci_test.sh")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--build-only", action="store_true", help="Only build the image")
    group.add_argument("--push-only", action="store_true", help="Only push the image (assumes image already built)")
    group.add_argument("--test-only", action="store_true", help="Only run ci_test.sh inside the container")
    
    args = parser.parse_args()

    


    if args.build_only:
        print("🚀 Building Docker image...")
        build_command = DockerBuild(args.project_name).build_command()
        run_cmd(build_command)
        print("✅ Build done.")
    elif args.push_only:
        docker = DockerBuild(args.project_name)
        run_cmd(docker.retag_command("prod"))
        run_cmd(docker.push_command(tag="prod"))
    elif args.test_only:
        test_command = DockerBuild(args.project_name).test_command(ci_test_path=args.ci_test_path)
        run_cmd(test_command)
    else:
        raise ValueError("Invalid argument")
