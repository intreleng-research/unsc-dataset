from bs4 import BeautifulSoup
import logging
import re

logger = logging.getLogger("unsc_db_filler")


class VetoHTMLParser:
    def __init__(self, html: str):
        self.html = html

    def create_resolution_to_veto_mapping_table(self) -> dict:
        """
        Reads the Veto table, and creates a mapping dict:
        {
            'Draft Resolution': ['Permanent member casting negative vote']
        }

        :return: A Dictionary which maps a draft resolution to a list of veto voters
        """

        """
        The structure of the page is like this:
        <tr>
            <th width="15%">Date</th>
            <th width="15%">Draft</th>
            <th width="20%">Written<br>
              Record</th>
            <th width="30%">Agenda Item</th>
            <th width="20%">Permanent<br>
              Member Casting<br>
              Negative Vote</th>
        </tr>

        In other words, we are interested in cell[1] (Draft Resolution) and cell[4] (Caster)

        The column of the draft resoltion (cell[1]) has a hyperlink to the draft resolution,
        sometimes some other text next to it. We'll want to take the <a>-tag's InnerText to
        only get the resolution id, and not other text.

        """
        soup = BeautifulSoup(self.html, "html5lib")
        rows = soup.table.find_all("tr")

        records = {}
        for row in rows:
            cells = row.find_all("td")

            # First few rows are about the Library..no real data.
            if len(cells) <= 1:
                continue

            resolution = cells[1].a.text
            veto_caster = cells[4].contents

            # This is interesting..and tricky..and annoying!!
            # If we'd use .text on cells[4], beautifulsoup converts the html in the <td> to a string.
            # <br/> tags are then ignored. This makes it impossible to sometimes recognize a country.
            # For example S/2020/667 is vetoed by China and Russian Federation.
            # The UN DAG Library has put a newline there using a <br>-tag.
            # .text gives then ChinaRussian Federation. Not reliable.
            # .contents on the other hand gives a list of elements found in the <td> tag:
            # ['China', <br/>, 'Russian Federation']. That middle element is not a string, it's a bs4.element.Tag.
            # And ideally we filter it out.
            # The only reliable way I have found thus far is to loop over the elements in the list,
            # find out the element's name, and if it's set to 'br', to decompose() it, as in: remove from the list.
            for el in veto_caster:
                if el.name == "br":
                    el.decompose()

            # Now normalize the entries
            veto_caster = [self.normalize_state_name(state) for state in veto_caster]

            records[resolution] = veto_caster

        return records

    def normalize_state_name(self, state_name: str) -> str:
        """
        Takes a state name and normalizes it. This is used as over the years for example
        Russia was referenced as both Russian Federation and as USSR.

        :param state_name: The name of the state
        :return: The normalized name of the state
        """
        normalization_table = {
            "Russian Federation": "Russia",
            "USSR": "Russia",
        }

        # If the passed state_name is found in our mapping table,
        # we need to normalize it. Normalize and then return that result
        if state_name in normalization_table.keys():
            return normalization_table.get(state_name.strip())

        return state_name.strip()
