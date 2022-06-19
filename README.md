This is an excerpt from the Location Scoring Tool code. Some of the files have been edited to remove unneeded dependencies for this purpose.

# Challenge
This challenge is all about the 'Industrial Buildings' class. This class reads data from an OpenStreetMap (OSM) file, finding buildings that we consider an 'Industrial Building'.

An 'Industrial Building' is a building that either:
	1. is tagged as 'building'='industrial' in OSM, or
        2. is in a landuse area tagged as 'landuse'='industrial', or
        3. is in a landuse area that has a building tagged as industrial, where the landuse is tagged with an 'industrial-like' tag (list of such tags defined in the unit tests)

Unit tests define this behaviour, and currently all pass. These unit tests 'fake' an OSM file to simulate each of the criteria listed.

However, in its current state, the 'Industrial Buildings' class does not do what we want: it will double count any buildings that meet more than one of the criteria above. For example, if there was a building tagged as "building"="industrial", which was also in a landuse area tagged as "landuse"="industrial", then that building would appear twice in the 'Industrial Buildings' class.

The challenge is to update this 'Industrial Buildings' class so that it does not double count buildings.

# Note
Whilst there are some concepts about OSM data that might be new to you, you won't need any of the geospatial concepts to do this challenge. What you do need to know is:
- that the osmium package allows you to iterate over all of the data in an OSM data file using the 'apply_file' function on a handler... that handler will then get it's 'node' function called for each node in the data file, then get its 'way' function called with each way in the file, then get its 'relation' function called with each relation in the file
- that both buildings and landuse areas are 'ways'
- that the way object also has its unique id as an attribute... you can use this id to spot duplicates
