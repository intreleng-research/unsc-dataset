# UNDataScraping
This folder is used by the program that builds the dataset from scratch. It scrapes the un.org website, and retrieves the html pages holding the yearly overview, and the meeting and resolution transcripts.
They are stored in the `scratch/`. 

The data used to build the current dataset are included for reference. We did not include the > 3GB of pdf text transcripts.
Those can be retrieved by running the `load_unsc_meeting_data_to_db.py` script.