''' Generate Airflow DAGs based on YAML templates.

    This function reads YAML files from the 'dynamic_dags' directory,
    renders them using Jinja2 templates, and saves the resulting DAGs
    as Python files in the 'dags' directory.
'''
from jinja2 import Environment, FileSystemLoader
import yaml
import os

# Set the Jinja2 environment and load the template
file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template("template_dag.jinja2")

# Iterate over all files in the 'dynamic_dags' directory
for filename in os.listdir(file_dir):
    if filename.endswith(".yaml"): # Only process YAML files
        with open(f"{file_dir}/{filename}", "r") as config_file:
            config = yaml.safe_load(config_file)
            with open(f"dags/jinja_get_data_{config['cloud']}.py", "w") as f:
                f.write(template.render(config))  # Parse the YAML file using the Jinja2 template 
