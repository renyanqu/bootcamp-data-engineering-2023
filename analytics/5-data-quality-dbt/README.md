# ****Bootcamp3 Data Quality with dbt and Postgres****

**Table of Contents**

- [Step 1: Clone the Repository](#step-1-clone-the-repository)
- [Step 2: Run Postgres Locally](#step-2-run-postgres-locally)
  - [Option 1: Run on Local Machine](#option-1-run-on-local-machine)
  - [Option 2: Run Postgres in Docker](#option-2-run-postgres-in-docker)
- [Step 3: Connect to Postgres](#step-3-connect-to-postgres)
- [Step 4: Create a python venv](#step-4-create-a-python-venv)
- [Step 5: Activate the python venv](#step-5-activate-the-python-venv)
- [Step 6: Install the dbt postgres adapter](#step-6-install-the-dbt-postgres-adapter)
- [Step 7: Point the dbt profile connection file path to zachs_shop](#step-7-point-the-dbt-profile-connection-file-path-to-zachs_shop)



## **Step 1: Clone the Repository**

Clone the repository.

## **Step 2: Run Postgres Locally**

First, you must install Postgres locally, regardless of which option you choose below.
- For Mac: Follow [this tutorial](https://daily-dev-tips.com/posts/installing-postgresql-on-a-mac-with-homebrew/) (Homebrew is recommended).
- For Windows: Follow [this tutorial](https://www.sqlshack.com/how-to-install-postgresql-on-windows/).

### **Option 1: Run on Local Machine**

- Execute the data.dump file to set up Postgres tables

    ```bash
    psql -U <computer-username> postgres < data.dump
    ```

    → Replace **`<computer-username>`** with your computer's username.


### **Option 2: Run Postgres in Docker**

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. Copy **`example.env`** to **`.env`** and update the values as needed

3. Start the Docker Compose container

    ```bash
    docker compose up -d

    # Short-cut if you're on Mac/Linux and have Make:
    make up
    ```

    > 💡 Check for a new folder, postgres-data, at the root directory. This folder stores data for your Postgres instance.
    >
4. Stop the Docker containers when finished

    ```bash
    docker compose down -v

    # Short-cut if you're on Mac/Linux and have Make:
    make down
    ```


## **Step 3: Connect to Postgres**

- Use your preferred client (DataGrip, VSCode, PGAdmin, Postbird, etc.) to establish a new PostgreSQL connection.
    - Default username: **`postgres`** (corresponds to **`$POSTGRES_USER`** in **`.env`**).
    - Default password: **`postgres`** (corresponds to **`$POSTGRES_PASSWORD`** in **`.env`**).
    - Default database: **`postgres`** (corresponds to **`$POSTGRES_DB`** in **`.env`**).
    - Default host: **`localhost`** or **`0.0.0.0`** (IP address of the Docker container).
    - Default port: **`5434`** (corresponds to **`$CONTAINER_PORT`** in **`.env`**).

> 💡 Edit these values by modifying the corresponding values in .env.
>

If the test connection is successful, click "Finish" or "Save" to save the connection. Now, you can use the database client to manage your PostgreSQL database locally.

## **Step 4: Create a python venv**

- Create a python virtual environment

    ```bash
    python3 -m venv venv
    ```

## **Step 5: Activate the python venv**

- Activate the python virtual environment

    ```bash
    source venv/bin/activate
    ```

## **Step 6: Install the dbt postgres adapter**

- Install dbt

    ```bash
    pip3 install dbt-postgres
    ```

## **Step 7: Point the dbt profile connection file path to zachs_shop**

```bash
export DBT_PROFILES_DIR=./
```

## **Step 8: Test the dbt connection**

- Test the dbt connection (If you changed some parameters in the .env file, you may need to change the same parameters in the profiles.yml file)

```bash
cd zachs_shop
dbt debug
```

You should be able to see the message "All checks passed!".


