from job import Job

import logging

logger = logging.getLogger("unsc_db_filler")


class JobQueue:
    """
    This class represents a FIFO queue which processes jobs.
    It has a list of jobs it needs to execute,
    a list of successfully processed jobs,
    a list of failed jobs (after having retried `retries` (by default 20) times).
    """

    def __init__(self, retries: int = 20) -> None:
        self.jobs: list = []
        self.processed: list = []
        self.failed: set = set()
        self.retries: int = retries

    def enqueue(self, job: Job) -> None:
        self.jobs.append(job)

    def dequeue(self, job: Job) -> None:
        self.jobs.remove(job)

    def size(self) -> int:
        return len(self.jobs)

    def process(self, function) -> None:
        """
        Try to process the jobs list using the passed function.
        Whenever the function completes without throwing an error,
        we assume it was successful. If we have an error running the function,
        we will retry the job, and add it back to the queue.

        If the function failed 20 times, we give up and add the job to failed jobs list.

        :param function: the function we want to run on the job
        """
        while True:
            if len(self.jobs) == 0:
                break

            logger.info("=====================================")
            try:
                # Take the oldest job from the queue
                job = self.jobs.pop(0)
                logger.info("Trying time %s for job %s", job.attempts, job)
                function(job)
                job.complete = True
            except Exception as e:
                logger.info(
                    "Failed to process job '%s': %s .. retrying later",
                    job.info(),
                    e,
                )

                # Increment failed attempts of job
                job.attempts += 1
                # Add the failed job again to the queue if it didn't fail the max retries times yet
                if job.attempts <= self.retries:
                    self.jobs.append(job)
                else:
                    # If we failed more than the max retry times, remove it from the queue, and list it as a failed job.
                    self.failed.add(job)
