from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, UniqueConstraint

from dbconnection import Base


class State(Base):
    """
    Models a Permanent Veto holding UN State
    """

    __tablename__ = "state"
    __table_args__ = (UniqueConstraint("name", name="state_name_uc"),)

    state_id = Column(Integer, primary_key=True)
    name = Column(String)

    # There's a many-to-many relation with Resolution, through a joint table 'VetoCasts'
    resolution = relationship("Resolution", secondary="vetocasts")
