
def generate_requirements_string(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            dependencies = [
                line.strip() for line in file if line.strip() and not line.strip().startswith('#')
            ]
            
            result = " ".join(dependencies)
            print(result)
            
    except FileNotFoundError:
        print(f"Error: File '{file_path}' does not exist.")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    generate_requirements_string('requirements.txt')

