# Database
- **database.env**: Where you store the credentials of your database. Set them before you run the DB the first time, and those will be used. If you are using an external Postgresql database server, update this file with the credentials.
- **docker-compose.yml**: For quickly getting a database server up and running. Install Docker and Docker Compose, and simply type `docker-compose up` in this folder to start.
- **export.sh**: A little helper script to export the data in your database (when you have made changes or are planning to make contributions :wink:). 