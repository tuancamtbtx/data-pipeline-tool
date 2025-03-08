import json

def load_json(file_path):
    """
    Load a JSON file and return its contents as a Python dictionary.
    
    :param file_path: The path to the JSON file.
    :return: A dictionary containing the JSON data.
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")