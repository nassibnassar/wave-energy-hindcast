-------------------------------------------------------------------------------
CONFIGURATION
-------------------------------------------------------------------------------

Rename hindcast-config as $HOME/.hindcast and edit it to configure the database
connection:

cp hindcast-config $HOME/.hindcast
chmod u=rw,g=,o= $HOME/.hindcast
vim $HOME/.hindcast

Create a user "hindcast_admin":

psql -U postgres -d template1 -c \
    "create role hindcast_admin with password '...password...' login inherit;"

-------------------------------------------------------------------------------
CREATING THE DATABASE
-------------------------------------------------------------------------------

First create a PostGIS database:

createdb -U postgres -O hindcast_admin hindcast
psql -U postgres -d hindcast -c \
    "create extension postgis;"
psql -U postgres -d hindcast -f postgis_owner.sql

Load the hindcast schema:

psql -U hindcast_admin -d hindcast -f hindcast.sql

-------------------------------------------------------------------------------
LOADING THE GRID
-------------------------------------------------------------------------------

Create a directory 'data' under this directory and copy the grid file there:

mkdir data
cp /...path-to-grid-file.../fort.14 data/

Run the grid loading script:

python load_grid.py

-------------------------------------------------------------------------------
LOADING THE MODEL DATA
-------------------------------------------------------------------------------

Run the model data loading script:

python load_model_data.py [variable] [year] [file_path]

E.g.:
python load_model_data.py tmm10 1999 /...path-to-file.../swan_TMM10.63

