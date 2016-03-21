# Google Fit importer for Correl8.me

## Quick start
Follow the [instructions here](https://developers.google.com/fit/rest/v1/get-started#request_an_oauth_20_client_id) and create a new app for accessing Fit API.

You will be calling the API from "Other UI" and accessing "User data".

Store the `client_secret.json` file in the directory where this README.md is located.

Run

    # initialize indices
    node app.js --init
    # allow API access to your fit data
    node app.js --authenticate client_secret.json

Follow the instructions in the console to authorize the app and store the OAuth token.

    # first run with an initial date
    node app.js --from 2001-01-01
    # for next runs, date range is automatic

Consider running the adapter from cron. Add something like the following into
your crontab (run `crontab -e`):

    0 * * * * /usr/bin/node <path to here/app.js

