import logging
import requests

from download_job import DownloadJob
from job_queue import JobQueue

logger = logging.getLogger("unsc_db_filler")


class DownloadFailed(Exception):
    pass


class HTMLDownloader:
    """
    This class is responsible for Downloading the UNSC pages we need to build our dataset.
    They are the general Meeting pages, and the Veto page.

    This class uses a Job Queue since the un.org web servers frequently give random errors.

    """

    def __init__(self, path: str = "./", since: int = 1946, until: int = 2021) -> None:
        self.path = path
        self.since = since
        self.until = until
        self.job_queue = JobQueue()

    def fetch_meeting_tables(self) -> None:
        """
        Fetches all the UN Meeting tables webpages between the year `self.since`
        and `self.until` to the location `self.path`.

        :param since: The first year to fetch the tables for
        :param until: The last year to fetch the tables for
        :return:
        """
        for year in range(self.since, self.until):
            # Some pages on the UN website end with .html, others with .htm...
            # it seems that prior to the year 1994, pages are .html, after that, .htm.
            # Since we do not want to check all these pages individually, we'll just try
            # and go to the .html version, and if that fails go to .htm
            if (
                year == 2022
            ):  # TODO: UN made it again inconsistent... we can query https://research.un.org/en/docs/sc/quick/meetings/2022
                # and query the iframe in which this page is rendered to figure out what the real url is we need to grab.
                url = f"https://www.un.org/depts/dhl/resguide/scact{year}_table_en.html"
            elif year < 1994:
                url = f"https://www.un.org/depts/dhl/resguide/scact{year}_table_en.html"
            else:
                url = f"https://www.un.org/depts/dhl/resguide/scact{year}_table_en.htm"

            # Originally we just tried html. If it failed, try .htm.
            # That trick was not reliable..the UN sometimes has both .html and .html..
            # but one is then incomplete!
            #
            # if res.status_code == 404:
            #    logger.info("Failed to fetch .html, retrying .htm...")
            #    url = f"https://www.un.org/depts/dhl/resguide/scact{year}_table_en.htm"
            #    res = fetch_url(url)
            #
            # Leaving in this comment as 'historical software engineering story telling'
            # https://www.youtube.com/watch?v=4PaWFYm0kEw&t=48s

            download_job = DownloadJob(
                url=url,
                dest_file=f"scact{year}_table_en.html",
                description=f"UNSC Meeting Table for Year `{year}`",
            )

            self.job_queue.enqueue(download_job)

            # In 2020 the UN started to document Covid remote meetings in another page.
            # Half their page was in the previous pages, half their page is on the new page:
            # https://www.un.org/depts/dhl/resguide/SC_2020-revised.html
            #
            # We create another downloadjob for 2020:
            if year == 2020:
                download_job = DownloadJob(
                    url="https://www.un.org/depts/dhl/resguide/SC_2020-revised.html",
                    dest_file=f"scact2020_covid_table_en.html",
                    description=f"UNSC Meeting Covid Table for Year `{year}`",
                )

                self.job_queue.enqueue(download_job)

        self.job_queue.process(self.download)

    def fetch_veto_table(self) -> None:
        """
        Enqueues the download job for downloading the veto table from the UNSC website,
        and starts processing it
        """
        url = "https://www.un.org/depts/dhl/resguide/scact_veto_table_en.htm"

        download_job = DownloadJob(
            url=url,
            dest_file=f"scact_veto_table_en.html",
            description=f"UNSC Veto Table",
        )
        self.job_queue.enqueue(download_job)
        self.job_queue.process(self.download)

    def download(self, job: DownloadJob) -> None:
        """
        Downloads the web page requested in a given DownloadJob
        and writes it to disk as the file specified in the DownloadJob
        This method is passed to the Job Queue process function to do the real heavy lifting

        :param job: the DownloadJob we use to download the real data we need
        """
        logger.info("Downloading %s - BEGIN", job.info())

        # No try..except here.. let it crash...
        # the job queue processing jobs needs to know when it failed
        res = self.fetch_url(job.url)

        logger.info("Downloading %s - END", job.info())

        if res.status_code == 200:
            self.write_to_disk(content=res.text, file=job.dest_file)
        else:
            raise DownloadFailed(
                f"Unable to download the html file for {job.dest_file}...status code was {res.status_code}"
            )

    def fetch_url(self, url: str) -> requests.Response:
        """
        Fetch a given url and return a requests Response

        :param url: the URL we want to fetch
        :return:    the requests Response
        """
        logger.info("Fetching %s", url)
        return requests.get(url)

    def write_to_disk(self, content: str, file: str) -> None:
        """
        Writes a given content for a given year, to a file called
        scact<year>_table_en.html in the SCRATCH_FOLDER location.

        :param content: The content that needs to be written to file
        :param file: the filename in which the content will be written
        """
        output_file = f"{self.path}/{file}"
        logger.info("Writing content to file '%s' BEGIN", output_file)

        with open(output_file, "w") as f:
            f.write(content)
            logger.info("Writing content to file '%s' END", output_file)
