import subprocess
from confluent_kafka.admin import AdminClient, NewTopic

def run_shell_command(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
    if result.returncode != 0:
        print(f"Error running command: {command}")
        print(result.stderr)
    else:
        print(result.stdout)
    return result.returncode

def main():
    # Run the Kafka installation script
    print("Running Kafka installation script...")

    # Apply Kubernetes configurations
    kubernetes_files = [
        'kafka-connect.yaml',
        'kafka-schema-registry.yaml'
    ]
    
    try:
        run_shell_command('./kafka-install.sh')
    except subprocess.CalledProcessError as e:
        print("Error running Kafka installation script")
        print(e.stderr)
        return

    try:
        for file in kubernetes_files:
            print(f"Applying Kubernetes configuration: {file}")
            run_shell_command(f'kubectl apply -f {file}')
    except subprocess.CalledProcessError as e:
        print("Error applying Kubernetes configuration")
        print(e.stderr)
        return

if __name__ == "__main__":
    main()