from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, Boolean, UniqueConstraint

from dbconnection import Base

# from resolution import Resolution


class Meeting(Base):
    """
    This class represents a UN Security Council Meeting.

    It has an id,
    it takes place in a certain year,
    at a certain date,
    in a meeting, there's one or more draft resolutions being discussed,
    which are related to a specific topic,
    and each meeting has a unique url where you can find the meeting transcript
    """

    __tablename__ = "meeting"
    __table_args__ = (UniqueConstraint("meeting_id", name="meeting_id_uc"),)

    meeting_id = Column(String, primary_key=True)
    topic = Column(String)
    full_text = Column(String)
    url = Column(String)
    date = Column(String)
    year = Column(Integer)
    veto_used_in_meeting = Column(Boolean)

    # Back Population, defining what is referring back to this table
    resolution = relationship("Resolution", back_populates="meeting")

    def __init__(
        self,
        meeting_id: str,
        year: int,
        date: str,
        topic: str = "",
        meeting_url: str = "",
        veto_used_in_meeting: bool = False,
    ) -> None:
        self.meeting_id = meeting_id
        self.year = year
        self.date = date
        self.topic = topic
        self.full_text = ""
        self.url = meeting_url
        self.veto_used_in_meeting = veto_used_in_meeting

    def add_meeting_transcript(self, text: str) -> None:
        self.full_text = text

    def __repr__(self) -> str:
        return f"id: {self.meeting_id} -- year: {self.year} -- topic: {self.topic} -- veto used: {self.veto_used_in_meeting} -- transcript: {'Available' if len(self.full_text) > 0 else 'N/A'}"
