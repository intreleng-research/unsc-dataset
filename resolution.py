from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, UniqueConstraint, ForeignKey
from typing import List

from dbconnection import Base
from meeting import Meeting


class Resolution(Base):
    """
    This class represents a UN Resolution.

    It all starts with a draft resolution in the form of S/XXXX/YYYY.
    This can become an adopted resolution in the form of S/RES/YYYY.
    A draft resolution can also be vetoed by a permanent members, not becoming adopted.

    """

    DRAFT = False
    FINAL = True

    __tablename__ = "resolution"
    __table_args__ = (UniqueConstraint("draft_id", name="draft_id_uc"),)

    id = Column(Integer, primary_key=True)
    draft_id = Column(String)
    final_id = Column(String)
    draft_url = Column(String)
    final_url = Column(String)
    status = Column(String)
    draft_text = Column(String)
    final_text = Column(String)
    year = Column(Integer)
    meeting_id = Column(String, ForeignKey("meeting.meeting_id"))

    meeting = relationship("Meeting", back_populates="resolution")
    state = relationship("State", secondary="vetocasts")

    def __init__(
        self,
        draft_id: str,
        final_id: str,
        kind: bool,
        adopted: bool,
        vetoed: bool,
        # vetoed_by: List[str],
        meeting: Meeting,
        year: int,
    ) -> None:
        self.draft_id: str = draft_id
        self.final_id: str = final_id
        self.type: bool = kind
        self.adopted: bool = adopted
        self.vetoed: bool = vetoed
        # self.vetoed_by: List[str] = vetoed_by
        self.draft_url: str = f"https://undocs.org/en/{self.draft_id}"
        self.final_url: str = (
            f"https://undocs.org/en/{self.final_id}"
            if self.final_id is not None
            else None
        )
        self.draft_text: str = ""
        self.final_text: str = ""
        self.meeting = meeting
        self.year: int = year

        if self.adopted:
            self.status = "adopted"
        elif self.vetoed:
            self.status = "vetoed"
            self.meeting.veto_used_in_meeting = True
        else:
            self.status = "not adopted"

    def add_draft_text(self, text: str) -> None:
        self.draft_text = text

    def add_final_text(self, text: str) -> None:
        self.final_text = text

    def __repr__(self) -> str:
        return f"draft: {self.draft_id} -- final: {self.final_id} -- vetoed: {self.vetoed} -- meeting: {self.meeting_id}"
