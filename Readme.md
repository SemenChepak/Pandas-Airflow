  # Before you begin
  This file contains several service definitions:
      airflow-scheduler - The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.
      airflow-webserver - The webserver is available at http://localhost:8080.
      airflow-worker - The worker that executes the tasks given by the scheduler.
      airflow-init - The initialization service.
      flower - The flower app for monitoring the environment. It is available at http://localhost:5555.
      postgres - The database.
      redis - The redis - broker that forwards messages from scheduler to worker. 
  
  # Deploy Airflow on Docker Compose
  ```bash
  git clone https://github.com/SemenChepak/Pandas-Airflow.git
  ```
  # Foobar

Foobar is a Python library for dealing with word pluralization.

  ## Installation

Use the package manager [pip](https://pidp.pypa.io/en/stable/) to install foobar.

```bash
pip install foobar
```

  ## Usage

```python
import foobar

# returns 'words'
foobar.sadaspluralize('word')

# returns 'geese'
foobar.pluralize('goose')

# returns 'phenomenon'
foobar.singularize('phenomena')
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
