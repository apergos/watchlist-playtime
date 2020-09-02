How (not) to dump a MediaWiki watchlist
=======================================

In order to get some other folks at Wikimedia initiated int othe mysteries of the xml/sql
dumps code, we chose a self-contained bug to play with, and everyone wrote some test
code. Here's my samples, writingin python instead of the approved MediaWiki php
that would normally be required for a maintenance script, because I could.

The format chosen was tsv, where we have the namespace (numeric), the title, and
the number of times that ns/title appears in the watchlist table.

History
-------

I started out doing the cheapest thing possible: write a script to dump batches of
rows from the watchlist table in compressed form, and then merge each of the output
files into the first of the output files, adding up the entries in the count column
and saving the final result. It became clear immediately that the unlimited growth of
the file being merged into, slowed the process down considerably after a point. Also,
php was going to be slower than a dedicated C utility; who knew? :-)

Next up, I used sort and datamash to sort the files to be merged and then merge them
together, adding the count columns as appropriate. This was done in batches of 100
files each, and then all of the files resulting from the 100 file merge batches were
merged together, resulting in the final output file. This was much faster. Of course
there was still no existence check, and having a two-part "run some python and then
some bash" was pretty untenable, even for a toy.

Doing the existence check meant dumping ns:title pairs out of the page database and
doing some clever sort of merge or join thing. The utility that met my needs was
mlr ("miller"), which I found could also be used in place of datamash, so I used
that everyplace. Unfortunately it could not do the sort -m piece, so I left that in.
Although this code did all the steps, it was suddenly much slower.

The final version of the script did all the steps and used datamash everywhere that
it could. This sped things up considerably.

Note that all the python scripts in here expect to be copied into a checked out
clone of https://github.com/wikimedia/operations-dumps and run from there.
In particular, they import some database-related classes that are used to
get credentials and establish a connection to the right database for the given
wiki, etc.

Don't actually use this for anything, please; it will likely eat your database
for a nice tasty snack. Thanks!
