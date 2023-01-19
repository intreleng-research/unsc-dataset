# The UN Security Council Dataset

This repository maintains a first dataset for the UN Security Council. It was first published in publication [ISBN 978-86-87043-84-8 doi: 10.52987/pcrc.2022](https://www.pcrconference.org/pcrc2022002.html).

We currently offer an overview of UNSC meetings, resolutions from its inception in 1946 until July 2022.
Only the meetings discussing a resolution are included. Presidential Statements and other meetings are not (yet) in this dataset.

## Dataset Design
The below is an entity relation diagram of the database holding the dataset.
![Entity Relation Diagram of the Database](./Database/ERDDatabase.png "Entity Relation Diagram of the Database")

A **Meeting** has:
  - a topic (topic)
  - a full meeting transcript (full_text)
  - a url where you can read it online (url)
  - a date when it took place (date)
  - the year it took place (year)
  - a possibility a veto was used in the meeting (veto_used_in_meeting)

A **Resolution** has:
  - been discussed in a specific meeting (meeting_id)
  - a draft id when it's a draft resolution (draft_id)
  - a final id when it's adopted (final_id) 
  - a url for both the draft and adopted resolutions (draft_url, final_url)
  - a status as outcome after voting: adopted, not adopted, vetoed (status)
  - text of its draft version (draft_text)
  - text of its adopted version (final_text)
  - a year it's been proposed (year)

A **State** has:
  - a unique id in the database (state_id)
  - a name (name)

A **Vetocast** has:
  - a resolution that was vetoed (vetoed_resolution)
  - the id of the state that cast the veto it (state_id)

## Dataset Usage

### SQL
The project has been written and executed in the following environment:

- Python >= 3.8.5
- Docker Compose 1.27.4
- Postgresql 10.4
- Jupyterlab >= 3.0.1 (M1 Mac users, definitely go for 3.4.3 minimum)

The dataset is currently a postgresql 10.4 backup. To start using it, you will need a postgresql 10.4 server running:

     $ cd Database
     $ docker-compose up
     Starting database_database_1 ... done
     Attaching to database_database_1
     database_1  | The files belonging to this database system will be owned by user "postgres".
     database_1  | This user must also own the server process.
     database_1  |  

You can then restore the [latest dataset export](/Database/db-export-2022-07-07.tar.gz):

      $ gunzip Database/db-export-2022-07-07.tar.gz
      $ pg_restore --host localhost --port 5432 --username "admin" --dbname "un" --role "admin" --verbose db-backup-2022-07-07.tar

When done, you should have table sizes comparable to the below.

    $ pgcli postgres://admin:admin@localhost/un
    admin@localhost:un> select count(*) from meeting;
    +-------+
    | count |
    |-------|
    | 2614  |
    +-------+
   
    admin@localhost:un> select count(*) from resolution
    +-------+
    | count |
    |-------|
    | 2690  |
    +-------+


### Jupyter Notebooks
In the `EDA/` folder there is an example `Exploratory_Data_Analysis` notebook. You can use it to see some of the results we made or as a template to get started.
Assuming `pandas` will be a library in your toolkit, the first step you most likely always want to do is connect to the database. You can do it using the below code:

```python
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Read the DB info and credentials from our .env file
load_dotenv("../Database/database.env")
DB_HOSTNAME = os.getenv('POSTGRES_HOSTNAME')
DB_USERNAME = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('POSTGRES_DB')

engine = create_engine(f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOSTNAME}/{DB_NAME}', echo=False)
meetings_df = pd.read_sql_table('meeting', con=engine)
resolutions_df = pd.read_sql_table('resolution', con=engine)
vetoes_df = pd.read_sql_table('vetocasts', con=engine)
states_df = pd.read_sql_table('state', con=engine)
```
It first reads the credentials from the `database.env` file, connects to it, and then reads every table into its own dataframe.
You will probably want to see the multiple tables in one single dataframe. You can use the merge function for this. 

Example:

```python
meetings_and_resolutions_df = resolutions_df.merge(meetings_df, how="inner", on=["meeting_id","year"])
```

More can be seen in the [EDA file](EDA/Exploratory_Data_Analysis.ipynb). To be able and run all examples, you will need to have the Facebooks Language Identification Model. You can download it for free here:
   ```shell
   $ curl https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin --output lid.176.bin
   ``` 

## (Re)Building the dataset from scratch

1. Install the environment
    ```shell script
    $ python3.8 -m venv unscd-venv
    $ source unscd-venv/bin/activate
    $ pip install -U pip 
    $ pip install -r requirements.txt
    $ python3.8 -m spacy download en_core_web_sm
    ```
    On MacOS - depending on the version used - it's possible you'll get a warning about OpenBLAS during the numpy installation.
    Install it through brew (`brew install openblas lapack`), and then re-run pip like this:
    ```shell script
    $ OPENBLAS="$(brew --prefix openblas) pip install -r requirements.txt 
    ```
    
    Optionally, you may have to reload your PATH in order for it to find your newly installed binaries.
    In your terminal, you can do this with `hash -r` (bash) or `rehash` (zsh)
   
2. Build the Dataset
    - Ask the UN Hammarskjöld library for the following data exports:
      - Search:
    https://digitallibrary.un.org/search?ln=en&rm=&ln=en&sf=year&so=d&rg=50&c=United+Nations+Digital+Library+System&of=hb&fti=0&fti=0&fct__2=Security+Council&p=089%3AB02
    MARC fields: 191__a, 239__a, 245__a, 245__c, 610__a, 650, 650__a, 856__u, 993__a
    
      - Search:
    https://digitallibrary.un.org/search?ln=en&p=089%3AB03+AND+089%3AB07&f=&action_search=Search&c=Resource+Type&c=UN+Bodies&sf=year&so=d&rg=50&fti=0&fct__2=Security+Council
    MARC fields: 191__a,610__a, 650__a, 856__u, 991_e, 992_a, 991
    
      - Search:
    https://digitallibrary.un.org/search?ln=en&p=089%3AB06&f=&rm=&ln=en&sf=year&so=d&rg=50&c=United+Nations+Digital+Library+System&of=hb&fti=0&fti=0&fct__2=Security+Council
    MARC fields: 191__a, 239__a, 245__a, 245__c, 269__a, 515__a, 610__a, 650__a, 856__u, 991__d
    
      They will reply with 3 Excel files. Let's call them `file1.xlsx`, `file2.xlsx`, and `file3.xlsx`These will be used in the next steps. 
    
    - Set the path to `file1.xlsx` in file `load_unsc_meeting_data_to_db.py`:
    
        `UN_LIBRARY_FILE1 = "UNExports/089_B02_-_SECURITY_COUNCIL_-_1st_Link.xlsx"`

    - Have a Postgresql 10.4 instance running. We used docker-compose to make it easier:
    ```
     $ cd Database
     $ docker-compose up
     Starting database_database_1 ... done
     Attaching to database_database_1
     database_1  | The files belonging to this database system will be owned by user "postgres".
     database_1  | This user must also own the server process.
     database_1  |  
     ...
    ```
    - Run Python script `load_unsc_meeting_data_to_db.py --fetch-all-unsc-tables`
    
      _Grab a cup of coffee, do some groceries, take your family out for dinner, refill that cup of coffee as it will take a couple of hours before it will be done (Expect a couple of hours)._

      Best results have been when you go in batches of a couple of years using the parameters `--since=` and `--until=`. Note that for the data analysis, we only look at data beyond 1994. Before 1994, some texts are OCR'd scans with mixed quality. Since 1994 the text is reliable. My recommendation is to start with `--since=2010 --until=2021` and then go down 10 years until you reach 1946. 
 
3. You can now start the Jupyter Notebooks
    ```shell
    $ jupyter-lab
    ```
   If charts or plots are not working, try and follow these steps:
    ```buildoutcfg
    $ export NODE_OPTIONS=--max-old-space-size=4096
    $ jupyter labextension install @jupyter-widgets/jupyterlab-manager --no-build
    $ jupyter labextension install plotlywidget --no-build
    $ jupyter lab build
    ```

## Contributing
All kinds of contributions are welcomed (eg: Python code updates, tests, Jupyter Notebooks with more examples, new data contributions to the dataset, ...). 
Contributions will be accepted through [pull requests](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests). 

> **Note:** If you are in the process of contributing something, we'd welcome you to open a [DRAFT pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests#draft-pull-requests) early in the process to increase awareness.

## Known Limitations
The following are limitations you need to be aware of when you use this dataset:
- Text data prior to 1994 is unreliable. Transcripts prior to 1994 were OCRd with mixed results. We welcome updates to these texts.
- The format of the meeting transcripts is dramatically different in the COVID-19 period due to remote meetings. This needs to be considered when doing textual analysis.
- The dataset only contains meetings in which a resolution was discussed. Presidential statements and other meetings are not included. Contributions are welcome.