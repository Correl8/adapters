# Google calendar importer for Correl8.me

## Quick start
Follow the [instructions here](https://developers.google.com/google-apps/calendar/quickstart/nodejs#step_1_turn_on_the_api_name) and create a new app for accessing calendars.

You will be calling the API from "Other UI" and accessing "User data".

Store the `client_secret.json` file in the directory where this README.md is located.

Run

    # initialize indices
    node app.js --init
    # allow API access to your calendar
    node app.js --authenticate client_secret.json

Follow the instructions in the console to authorize the app and store the OAuth token.

    # first run with an initial date
    node app.js --from 2001-01-01
    # for next runs, date range is automatic

Consider running the adapter from cron. Add something like the following into
your crontab (run `crontab -e`):

    0 * * * * /usr/bin/node <path to here/app.js

