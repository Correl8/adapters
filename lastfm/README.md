# Last.FM importer for Correl8.me

## Quick start
Create an API account [here](http://www.last.fm/api/account/create)

Run

    # initialize indices
    node app.js --init
    # allow API access to your last.fm data
    node app.js --authenticate

Follow the instructions in the console to authorize the app and store the OAuth token.

    # first run with an initial date
    node app.js --from 2001-01-01
    # for next runs, date range is automatic

Consider running the adapter from cron. Add something like the following into
your crontab (run `crontab -e`):

    0 * * * * /usr/bin/node <path to here>/app.js

