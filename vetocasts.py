from sqlalchemy.orm import relationship, backref
from sqlalchemy import Column, String, Integer, UniqueConstraint, ForeignKey

from dbconnection import Base
from resolution import Resolution
from state import State


class VetoCasts(Base):
    __tablename__ = "vetocasts"
    __table_args__ = (
        UniqueConstraint(
            "vetoed_resolution", "state_id", name="vetoed_resolution_state_id_uc"
        ),
    )

    vetoed_resolution = Column(
        String, ForeignKey("resolution.draft_id"), primary_key=True
    )
    state_id = Column(Integer, ForeignKey("state.state_id"), primary_key=True)

    resolution = relationship("Resolution", backref=backref("vetocasts"))
    state = relationship("State", backref=backref("vetocasts"))

    def __init__(self, vetoed_resolution: str, state_id: int) -> None:
        self.vetoed_resolution = vetoed_resolution
        self.state_id = state_id
