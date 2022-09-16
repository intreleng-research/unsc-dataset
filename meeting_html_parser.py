from bs4 import BeautifulSoup
import logging
import re

logger = logging.getLogger("unsc_db_filler")


class MeetingHTMLParser:
    def __init__(self, html: str):
        self.html = html

    def extract_records(self) -> list:
        """
        Parses the html of a UNSC meeting table page.
        :param html: The html of an UNSC meeting table page
        :return: a list of records (of the tye dict) representing UN records found on the UNSC meeting tables
        """

        """
        Even though the summary of the tables of 1946 and 2020 are the same, it seems that in __1994__, content
        was moved around.

        1946:
            <tr>
                  <th width="15%">Meeting<br />Record</th>
                  <th width="15%">Date</th>
                  <th width="30%">Topic</th>
                  <th width="25%">Security Council<br> Outcome / Vote</th>
            </tr>

        2020:
            <tr>
                <th width="15%">Meeting<br />Record</th>
                <th width="15%">Date</th>
                <th width="15%">Press<br />Release</th>
                <th width="30%">Topic</th>
                <th width="25%">Security Council<br> Outcome / Vote</th>
            </tr>

        => prior to 1994, we want all columns. As from 1994, we skip the 3rd column.
        """

        soup = BeautifulSoup(self.html, "html5lib")
        rows = soup.table.find_all("tr")

        records = []

        for row in rows:
            cells = row.find_all("td")
            if len(cells) == 0:
                continue
            # If we have 7 columns, we're looking at the table structure of COVID times
            # Not every meeting has a S/PV.XXXX record number then.
            # We take the record and document behind "Letter with Vote Procedure / Briefings"
            # as it contains the most complete information wrt the virtual meeting.
            if len(cells) == 7:
                if re.findall(
                    "S/[0-9]{1,5}|S/[0-9]{4}\/[0-9]{1,4}|S/RES/[0-9]{1,4}\s?\(?[0-9]{4}\)?",
                    cells[6].get_text(),
                ):
                    # If there's no meeting record (S/PV.XXXX), the virtual meeting
                    # text is and voting is captured in full in the
                    # "Letter with Vote Procedure / Briefings" document
                    # This is captured in column 6.
                    meeting_record = cells[0].get_text()
                    # If the first column has --, there's no S/PV.XXXX record.
                    # We look at the 6th column:
                    if meeting_record == "--":
                        if cells[5].get_text() != "--":
                            meeting_record = cells[5].get_text().replace("\n\t\t", "")
                            # If there are multiple records listed, we will only add the first one
                            # Example: 16 April 2021: S/2021/373 and S/2021/372 (on the same topic)
                            meeting_record = re.match('S\/[0-9]{4}\/[0-9]{0,3}', meeting_record).group(0)
                            meeting_url = cells[5].a["href"]
                        else:
                            # If the 6th column also has --, then we skip it, it was a private meeting.
                            # Example 8 June 2020, S/RES/2580 (2021). No data at all available... .
                            continue
                    else:
                        meeting_url = cells[0].a["href"]

                    records.append(
                        {
                            "meeting_record": meeting_record,
                            "meeting_url": meeting_url,
                            "date": cells[1].get_text(),
                            "topic": cells[3].get_text(),
                            "outcome": cells[6].get_text(),
                        }
                    )

            # if we have more than 4 cells, we're in the post 1994 timeframe
            # hence, skip the 3rd cell!
            elif len(cells) == 5:

                # Now if the last column is a (Draft)Resolution, we will consider it.
                # Format of a Draft Resolution is: `S/YYYY/X` - `S/YYYY/XXXX` where YYYY is the year
                # and X is the Xth draft resolution of that year.
                #
                # In earlier years, for example 1989, some draft resolutions also are of the format
                # S/X - S/XXXX
                #
                # Format of an approved Resolution is: `S/RES/X` - `S/RES/XXXX (YYYY)` where X is the number
                # and YYYY is the year
                if re.findall(
                    "S\/[0-9]{1,5}|S\/[0-9]{4}\/[0-9]{1,4}|S\/RES\/[0-9]{1,4}\s?\(?[0-9]{4}\)?",
                    cells[4].get_text(),
                ):
                    records.append(
                        {
                            "meeting_record": cells[0].get_text(),
                            "meeting_url": cells[0].a["href"],
                            "date": cells[1].get_text(),
                            "topic": cells[3].get_text(),
                            "outcome": cells[4].get_text(),
                        }
                    )
            # We're in the pre-1994 table structure.
            elif len(cells) == 4 and re.findall(
                "S/PV.[0-9]{1,4}|S/[0-9]{1,5}|S/[0-9]{4}/[0-9]{1,4}|S/RES/[0-9]{1,4}\s?\(?[0-9]{4}\)?",
                cells[3].get_text(),
            ):
                # Sometimes there's no meeting record (see early years, eg: 1948)
                # We can't store the data then as we can't relate in which meeting
                # a resolution was voted... .
                if cells[0].get_text() == "":
                    continue

                records.append(
                    {
                        "meeting_record": cells[0].get_text(),
                        "meeting_url": cells[0].a["href"],
                        "date": cells[1].get_text(),
                        "topic": cells[2].get_text(),
                        "outcome": cells[3].get_text(),
                    }
                )

        return records
