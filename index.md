# Index.py

### File Structure

All the pages used in the app are standalone dash apps.
The index page imports each app's layout and renders them to the user via navigation
of the collapsable sidebar on the left of the app screen.

- apps
-- contains all app scripts for individual pages.

- assets
-- contains global css files for styling of index page.

- index.py
-- main page for the app.

- app.py
-- generic app content renderer (Dash backend).

- requirements.txt
-- list of package dependencies required for installation.

- development
-- space set aside for development isolated from main app.
