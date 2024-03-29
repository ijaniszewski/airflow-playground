FROM python:3.10-slim-buster

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    && apt install -y git-all
# install git :)

WORKDIR /usr/src/test_dbt_project

# Install the dbt Postgres adapter. This step will also install dbt-core
RUN pip install --upgrade pip
RUN pip install pytz dbt-postgres==1.3.1

# Install dbt dependencies (as specified in packages.yml file)
# Build seeds, models and snapshots (and run tests wherever applicable)
# CMD dbt deps && dbt build --profiles-dir profiles && sleep infinity
CMD sleep infinity