# BookOps Workshop: Airflow ETL/ELT Pipelines

Hands-on workshop scaffold for teaching Apache Airflow 3.2 through a fictional BookOps data platform.

[Slide deck (PDF)](slides/workshop-slides.pdf)

> IMPORTANT 🚨: Please complete the following setup before the tutorial. We need to pull(download) docker images and install packages, let's do it faster at the comfort of our home WIFI.

## Setup 

### Prerequisites

- Python 3.12
- Docker Desktop with at least 4 GB memory

### Using Github Codespaces

If your local system doesn't allow you to install things, you can use github codespaces. Click on Code(top right) -> Codespaces -> Create new codespaces on main

![as an alternative use github codespaces](assets/images/github-codespaces.png)

If you use codespace instead of using localspace you will use something like this `https://humble-enigma-4xvq5wgx66cqppv-8080.app.github.dev/`
Replace - `humble-enigma-4xvq5wgx66cqppv` - with your unique codespace name

PORTS
- 8080 for airflow
- 5432 for postgres
- 8501 for analytics app

### Goal: Airflow Environment

To get the Airflow home page after logging in
![airflow home screen screenshot](assets/images/airflow-home-screen.png)

### Clone this repo

```
git clone git@github.com:thelearningdev/pyconus-2026-apache-airflow-tutorial.git
```

### Setup Local Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
export AIRFLOW_HOME=$PWD # important if not airflow will take your home folder for setting up airflow
pip install --upgrade pip
pip install -r requirements.txt
```

### Docker Compose

We will use `docker-compose` to run our airflow system, a postgres db and an analytics app

```bash
docker compose up --build
```

## Setup Check


### 1. Airflow is up and Running

Open [http://localhost:8080](http://localhost:8080)

![airflow login screen screenshot](assets/images/airflow-login.png)

and sign in with 


```
username: admin
password: <find-in-file-simple_auth_manager_passwords.json.generated>
```

![airflow home screen screenshot](assets/images/airflow-home-screen.png)


### 2. Airflow Shell

On a new terminal run

```
docker compose exec airflow /bin/bash
```
On the shell that opens run

---

```
airflow dags list
```
You will see a list of all the dags in the `dags/` folder

### 3. Postgres DB tables

You can use a postgres client like pgadmin or dbeaver(my favorite) or any other postgres explorer of your choice
You need to connect two db's one is for our data engineering stuff, one is used by airflow.
After connecting to the DB you should able to explore the tables like below

![dbeaver screenshot sample after connecting to the DBs](assets/images/dbeaver-tables.png)

> Note that you're connecting to two databases running in your docker container. One to load the books data another to hold airflow related information. O

```
postgresql+psycopg://airflow:airflow@localhost:5432/airflow
postgresql://airflow:airflow@localhost:5432/bookops
```

### 4. Analytics App

1. Open [http://localhost:8501/](http://localhost:8501/) on your browser.
2. You will see `BookShop Pipeline Dashboard`
3. Don't worry about the errors, we run the pipeline to fill up this dashboard.


> End of Setup. The rest we can do it at the workshop
  

## Exercises
- [All Exercises are in exercise.md](./exercises.md)

## Notes

1. Anytime Airflow feels clumsy run we can reset airflow db for a cleaner setup

```
docker-compose exec airflow /bin/bash
```

Will open the airflow container shell. In there run

```
airflow db reset -y
airflow db migrate
airflow dags reserialize
```

2. If your tasks are not getting scheduled or running for any reason, restart the airflow terminal/docker container, you should be good. Since we are using standalone version, rarely everything gets mushed together

