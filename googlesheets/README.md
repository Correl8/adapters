# Google sheets importer for Correl8.me

## Quick start
Use [this wizard](https://console.developers.google.com/start/api?id=sheets.googleapis.com) to create or select a project to access the sheets.

You will be calling the API from "Other UI" and accessing "User data".

Store the `client_secret.json` file in the directory where this README.md is located.

Run

    # initialize indices
    node app.js --init
    # allow API access to your sheets
    node app.js --authenticate client_secret.json

Follow the instructions in the console to authorize the app and store the OAuth token.

Now you're ready to run the adapter.

    # first run with an initial date
    node app.js --from 2001-01-01
    # for next runs, date range is automatic and you can just call
    node app.js

Consider running the adapter from cron. Add something like the following into
your crontab (run `crontab -e`):

    0 * * * * /usr/bin/node <path to here>/app.js
