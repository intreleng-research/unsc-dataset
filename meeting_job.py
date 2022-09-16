from job import Job


class MeetingJob(Job):
    """
    A Meeting job starts with a meeting record. A meeting records looks like this:

    {
      'meeting_record': 'S/PV.8697',
      'meeting_url': 'https://undocs.org/en/S/PV.8697',
      'date': '20 December',
      'topic': 'The situation in the Middle East',
      'outcome': 'Draft resolution S/2019/961 vetoed by Russian Federation and China\n\t13-2-0 \n\tS/2019/962 not adopted5-6-4'
    }

    It can be enqueued to a `Queue` instance for retrying when failing.

    """

    def __init__(self, meeting_record: dict, description: str = "") -> None:
        super().__init__(description=description)
        self.meeting_record: dict = meeting_record

    def info(self):
        return self.meeting_record.get("meeting_record")

    def __repr__(self) -> str:
        return f"Job for meeting {self.meeting_record['meeting_record']} in year {self.meeting_record['year']}"
