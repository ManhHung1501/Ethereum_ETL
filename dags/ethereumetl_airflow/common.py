import os 

def get_project_directory() -> str:
    return os.path.abspath(os.path.join(__file__, "../../.."))
project_dir = get_project_directory()

