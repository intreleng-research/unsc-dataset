from sqlalchemy import create_engine

from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.pool import StaticPool

import logging

logger = logging.getLogger("unsc_db_filler")

Base = declarative_base()


class DBConnection:
    def __init__(self, host: str, dbname: str, user: str, password: str) -> None:
        self.host: str = host
        self.dbname: str = dbname
        self.user: str = user
        self.password: str = password
        self.engine: Engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}",
            echo=True,
            poolclass=StaticPool,
        )
        self.session: Session = sessionmaker(bind=self.engine)()
        Base.metadata.create_all(self.engine)

    @property
    def connection_string(self) -> str:
        return f"host={self.host} dbname={self.dbname} user={self.user} password={self.password}"

    def get_session(self) -> Session:
        return self.session

    def get_engine(self) -> Engine:
        return self.engine
