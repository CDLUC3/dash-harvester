dash-harvester
==============

Python script to parse  atom feed from Merritt repository for harvesting by XTF.

Requires Python 2.7.x to be installed

Atom Feeds live in /apps/dash/apps/dash-harvester/harvest.csh
 
Each of the commands has the form:
/dash/local/bin/python ${harvestbase}/parseFeed14.py \
        "https://merritt.cdlib.org/object/recent.atom?collection=ark:/13030/m5q82t8x" \
        ucb
 
The first is the python script, the second is the Merritt feed URL, and the third is the campus. 

