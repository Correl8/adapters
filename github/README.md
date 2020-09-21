# Github importer for Correl8.me

## Quick start

[https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/](Create a personal access token for the command line). Grant the scopes `repo:status` and `read:user`.


### Initialize

    # initialize indices
    npm start --init
    # allow API access to your data
    npm start --settings

Follow the instructions in the console to authorize the app and store the OAuth token.

### Run
    npm start

### Automate

Consider running the adapter from cron. Add something like the following into
your crontab (run `crontab -e`):

    0 * * * * /usr/bin/node <path to adapters>/github/node_modules/correl8-cli/app.js -a <path to adapters>/github

