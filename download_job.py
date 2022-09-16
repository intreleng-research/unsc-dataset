from job import Job


class DownloadJob(Job):
    """
    A Download job has a url it needs to go and download. As well as a destination file to save the download to.
    It can be enqueued to a `Queue` instance for retrying when failing.

    """

    def __init__(self, url: str, dest_file: str, description: str = "") -> None:
        super().__init__(description=description)
        self.url: str = url
        self.dest_file: str = dest_file

    def info(self):
        return f"Job for {self.description} from {self.url} to {self.dest_file}"

    def __repr__(self) -> str:
        return f"Job for {self.description} from {self.url} to {self.dest_file}"
