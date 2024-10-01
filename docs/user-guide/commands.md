# Commands

## User commands

These commands can be invoked by any user in the server. All commands begin with `o` to prevent command collisions with other bots.

### Add user

`/omember add` : Add the invoking user to the bot's database.

### Verify user

`/omember verify` : Verify the invoking user so only they, or bot administrators, can edit their database entries.

### Edit user

`/omember edit OLD_NAME NEW_NAME` : Update the invoking user's name in the bot's database.

### Add mount(s)

`/oadd KIND NAMES` : Add mounts to the invoking user. `KIND` must be one of `trials` or `raids`, indicating the kind of mount to add, and `NAMES` must be the name of the mount to add, or a comma-separated list of mount names.

Examples:

- `/oadd trials ifrit` - Adds Ifrit to the user's mounts.
- `/oadd trials ifrit,titan` - Adds Ifrit and Titan to the user's mounts.
- `/oadd raids o8s,o12s` - Adds Omega 8 savage and Omega 12 savage to the user's mounts.

### Remove mount(s)

`/oremove KIND NAMES` : Remove mounts to the invoking user. `KIND` must be one of `trials` or `raids`, indicating the kind of mount to remove, and `NAMES` must be the name of the mount to add, or a comma-separated list of mount names.

Examples:

- `/oremovetrials garuda` - Removes Garuda from the user's mounts.
- `/oremove trials ramuh,leviathan` - Removes Ramuh and Leviathan from the users mounts.
- `/oremove raids a4s,a12s` - Removes Alexander 4 savage and Alexander 12 savage from the user's mounts.

### Display mount(s)

`/odisplay KIND(optional) EXPANSION(optional)` : Display a table showing the mounts a user has. If `KIND` is specied, it must be one of `trials` or `raids`, and only mounts from that kind will be shown. If `EXPANSION` is specified, it must the the name of an FFXIV expansion and only mounts from that expansion will be shown.

Examples:

- `/odisplay` - Displays all mounts the user has, from trials and raids, from all expansions.
- `/odisplay trials` - Displays mounts the user has from trials, from all expansions.
- `/odisplay raids stormblood` - Displays mounts the user has, from raids, from Stormblood.

### Show names

`/onames KIND` : Display a list of the names compatible with bot commands. `KIND` must be one of `mounts` or `expansions`, and controls which list of bot-compatible names will be shown.

### Help

`/ohelp` : Displays this page as a message.
