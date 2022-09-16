from fake_useragent import UserAgent
import logging
import os
import shutil
import requests
import magic
import mimetypes
import requests
from lxml import html
from urllib.parse import urljoin

logger = logging.getLogger("unsc_db_filler")


class DownloadFailed(Exception):
    pass


class PDFDownloader:
    """
    This class is responsible for Downloading the PDFs holding meeting and resolution transcripts we need
    to build our dataset.

    **Update 03 July 2022**: The UN has changed how they're serving files to people in an 'interesting'
    way. Downloading files goes through multiple 'redirects'. Not pure 302 responses, but actual 200 OKs,
    with a <meta> tag refreshing the page to another page 1 second later.

    Tracing the events in a browser also reveal that it's loggin in into a specific server using a username
    and password (that apparently is not secret..the point of it is void as such), to receive a bunch of cookies
    needed to ask the final PDF download.

    """

    def __init__(self, path: str = "./") -> None:
        self.path: str = path

    def get_cookie_string(self) -> str:
        """
        Return a cookie string to authenticate the PDF download against the UN.org data serving platform

        :return: the cookie string
        """

        uri = "https://documents-dds-ny.un.org/prod/ods_mother.nsf?Login&Username=freeods2&Password=1234"

        # Do NOT follow the redirect..the cookies we need are returned in the 302!!
        r = requests.get(uri, allow_redirects=False)

        cookie_string = ""
        for cookie, cookie_val in r.cookies.items():
            cookie_string += f"{cookie}={cookie_val};"

        return cookie_string

    def test_for_meta_redirections(
        self, r: requests.Response
    ) -> (bool, requests.Response):
        """
        Test if in a HTTP response there are Meta-Refresh tags used.

        :param r: the response to be evaluated
        :return: (True, new URL) if there is a redirect in the response found, (False, None) if no redirects can be found any longer.
        """
        # Credits to https://stackoverflow.com/questions/2318446/how-to-follow-meta-refreshes-in-python
        mime = magic.from_buffer(r.content, mime=True)
        extension = mimetypes.guess_extension(mime)

        if extension == ".html":
            html_tree = html.fromstring(r.text)
            refresh_attribute = html_tree.xpath(
                "//meta[translate(@http-equiv, 'REFSH', 'refsh') = 'refresh']/@content"
            )

            # In the UN their amazing meta-redirects, you can end up with a 200 OK that doesn't return a PDF.
            # Instead, you can get a web page saying the document doesn't exist.
            # The Digital Library personnel informed me that all documents need to be translated to multiple languages
            # and this can take a few days... . As such
            if len(refresh_attribute) == 0:
                raise DownloadFailed(f"The download failed...is the document you're trying to fetch maybe not yet available?")

            # example attr: ['1; URL=/tmp/6596179.00848389.html']
            refresh_attribute = refresh_attribute[0]
            wait, text = refresh_attribute.split(";")
            text = text.strip()
            if text.lower().startswith("url="):
                url = text[4:]
                if not url.startswith("http"):
                    # Relative URL, adapt
                    url = urljoin(r.url, url)
                return True, url

        return False, None

    def follow_redirections(
        self, r: requests.Response, s: requests.Session
    ) -> requests.Response:
        """
        Recursive function that follows meta refresh redirections if they exist in a given HTTP response

        :param r: The Response to be evaluated for meta refresh redirections
        :param s: The Session to use

        """
        redirected, url = self.test_for_meta_redirections(r)
        if redirected:
            cookie = self.get_cookie_string()
            headers = {"Cookie": cookie}
            r = self.follow_redirections(s.get(url, headers=headers), s)

        return r

    def download_pdf(self, what_to_fetch: str, uri: str = None) -> str:
        """
        Downloads a resolution or meeting transcript, and saves it to disk.
        It returns the filename written.

        :param what_to_fetch: the original ID of the meeting or resolution to download
        :param
        :return: the filename written to disk
        """
        file_to_write = what_to_fetch.replace(".", "_")
        file_to_write = file_to_write.replace("/", "_")

        if not os.path.exists(f"{self.path}"):
            os.makedirs(f"{self.path}")

        file_to_write = f"{self.path}/{file_to_write}"

        if uri == None:
            uri = f"http://www.undocs.org/en/{what_to_fetch}"

        s = requests.Session()
        r = s.get(uri, allow_redirects=True)
        with self.follow_redirections(r, s) as r:
            if r.status_code == 200:
                # Of course... the UN now always returns a 200 OK... even if what we ask is nonsense...
                # great....... So now we need to check if what they return is actually a PDF or not.
                content_type = r.headers.get('Content-Type')
                logger.info(f"Content-type for '{what_to_fetch}' is: %s", content_type)
                if content_type != 'application/pdf':
                    raise DownloadFailed(
                        f"Unable to download {what_to_fetch}...we received content-type '{content_type}' instead of 'application/pdf'"
                    )
                with open(file_to_write, "wb") as f:
                    f.write(r.content)
            else:
                logger.info(
                    f"Unable to download {what_to_fetch}...status code was {r.status_code}"
                )
                raise DownloadFailed(
                    f"Unable to download {what_to_fetch}...status code was {r.status_code}"
                )

        return file_to_write
