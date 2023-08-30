import os
import sqlite3
import datetime
from typing import Generator
import pandas as pd
from time import time


def short_timeit(function):
    """Used for display a time of solution run
    """
    def wrapper(*args, **kwargs):
        ts = time()
        result = function(*args, **kwargs)
        te = time()
        res = (te-ts) * 1000000
        print(f'Short timeit: {res} mks')
        return result
    return wrapper


def get_db(
    url: str = './src/data/cheaters.db'
        ) -> Generator[sqlite3.Connection, None, None]:
    """Get db connection

    Args:
        url (str, optional): path to db. Defaults to 'cheaters.db'.

    Yields:
        Connection
    """
    path = os.path.abspath(url)
    with sqlite3.connect(path) as db:
        yield db


def add_task_table(db: sqlite3.Connection) -> None:
    """Add task_table to db
    """
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE task_table (
            timestamp TIMESTAMP,
            player_id INTEGER PRIMARY KEY,
            event_id TEXT,
            error_id TEXT,
            json_server TEXT,
            json_client TEXT,
            FOREIGN KEY(player_id) REFERENCES cheaters(player_id)
            )
        """
        )
    cur.close()


def get_cheaters(db: sqlite3.Connection) -> pd.DataFrame:
    """Get cheaters data from db

    Returns:
        pd.DataFrame
    """
    c = pd.read_sql_query("SELECT * FROM cheaters", db)
    c['ban_time'] = c['ban_time'].astype('datetime64[ns]')
    return c


@short_timeit
def merge_data(
    cheaters: pd.DataFrame,
    client: str = './src/data/client.csv',
    server: str = './src/data/server.csv',
        ) -> pd.DataFrame:
    """Construct data for new database table

    Args:
        cheaters (DataFrame): data from existed table of cheaters
        client (str): path to client.csv. Default to ./src/data/client.csv
        server (str): path to server.csv. Default to ./src/data/server.csv

    Returns:
        pd.DataFrame
    """
    def compare(_id: int) -> bool:
        one = ex.query(f'player_id=={_id}')[['ban_time', 'timestamp']]
        if len(one) > 0:
            s = one.iloc[0].timestamp
            if one.iloc[0].ban_time < datetime.datetime(
                s.year, s.month, s.day
                    ):
                return False
        return True

    cf = pd.read_csv(os.path.abspath(client))
    sf = pd.read_csv(os.path.abspath(server))
    cf.drop(columns=['timestamp'], inplace=True)
    sf['timestamp'] = sf['timestamp'].astype('datetime64[ns]')
    conc = pd.merge(
        cf, sf, how='inner', on='error_id', suffixes=['_client', '_server']
            )
    conc.rename(
        columns={
            "description_client": "json_client",
            "timestamp_server": "timestamp",
            "description_server": "json_server"
            },
        inplace=True
            )
    ex = pd.merge(conc, cheaters, how='inner', on='player_id')
    conc.drop(ex[ex['player_id'].apply(
        lambda x: compare(x))].index, inplace=True
              )
    return conc


def main(db: sqlite3.Connection) -> None:
    """Runer for a script
    """
    try:
        add_task_table(db)
    except sqlite3.OperationalError:
        print('Table task_table exist. Data droped and replaced.')
    cheaters = get_cheaters(db)
    conc = merge_data(cheaters)
    conc.to_sql('task_table', db, if_exists='replace', index=False)
    print('Data is inserted to db.')


if __name__ == '__main__':
    db = next(get_db())
    main(db)
