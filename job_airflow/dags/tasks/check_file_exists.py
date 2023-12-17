import os
import fnmatch

def find_matching_file(folder_path, pattern, **kwargs):
    """
    Find and return the first file name in the specified folder
    that matches the given pattern.

    Parameters:
    - folder_path (str): Path to the folder to search in.
    - pattern (str): The pattern to match against file names.

    Returns:
    - str or None: The matched file name, or None if no match is found.
    """
    try:
        # List all files in the folder
        files = os.listdir(folder_path)

        # Find the first file that matches the pattern
        for file in files:
            if fnmatch.fnmatch(file, pattern):
                file_path = f'{folder_path}/{file}'
                kwargs['ti'].xcom_push(key='daily_table', value=file_path)
                print(file_path)
                return

    except OSError as e:
        print(f"Error reading files in {folder_path}: {e}")

