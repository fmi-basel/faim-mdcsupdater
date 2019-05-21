# FAIM-MDCStoreUpdater

FAIM-MDCStoreUpdater provides functionality to change file locations in an MDCStore database for images that were copied or moved.

NOTE The updater currently only works with MS-SQL databases as backend of the MDCStore and has not yet been tested extensively.

## Installation

```
cd PATH/TO/faim-mdcsupdater
pip install .
```

If you want to use this as a plugin in [FAIM-Robocopy](https://github.com/fmi-basel/faim-robocopy), make sure to place both ```plugin.py``` and ```mdcstore_updater.ini``` in FAIM-Robocopy's subfolder ```plugins/faim-mdcsupdater/```. Adjust the settings in ```mdcstore_updater.ini``` according to your database.


## Usage

The updater can be used as plugin in [FAIM-Robocopy](https://github.com/fmi-basel/faim-robocopy) or as a standalone script.

For the latter, assume that the database the information that some images are at ```old/location/of/images```, but that these images were in the meantime moved to ```new/location/of/images```. You can now update these database entries with:

```
cd PATH/TO/faim-mdcsupdater
python run_updater.py --config path/to/mdcstore_updater.ini --source old/location/of/images --dest new/location/of/images
```