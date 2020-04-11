# Building a Model

In order to run this notebook locally, please follow these instructions. Otherwise you can [visualize the notebook on your browser](./twitter-sentiment-analysis.ipynb).

```bash
# Create a virtual environment to isolate this project from other Python projects and avoid dependency conflicts
virtualenv -p python3 venv

# Activate your virtual env. You will see a (venv) before your usual terminal prompt
source venv/bin/activate

# If you want to use Jupyter and have it installed in this virtual environment
pip install jupyter

# Install the single main dependency
pip install nltk==3.4.5

# Get Jupyter started then select the notebook 'twitter-sentiment-analysis.ipynb' in the list
jupyter-notebook
```