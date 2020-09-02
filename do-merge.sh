#!/bin/bash

# do the merge using the previously generated files via datamash

cd /var/lib/dumpsgen/

for i in $( seq 1 9 ); do
    filenames=$( ls /srv/tmp2/wikidatawiki/wikidatawiki-watchlist-${i}*gz )
    echo -n | gzip > blot.gz
    date
    echo "doing files starting with $i"
    for filen in $filenames; do
	sort -t$'\t' -k2n -k3  -m <(zcat /var/lib/dumpsgen/blot.gz) <(zcat $filen) |  datamash -g 1,2 sum 3 | gzip > /var/lib/dumpsgen/blot2.gz
	mv /var/lib/dumpsgen/blot2.gz /var/lib/dumpsgen/blot.gz
    done
    date
    mv /var/lib/dumpsgen/blot.gz /var/lib/dumpsgen/interm-output-${i}.gz
done
echo "doing final merge"
filenames=$( ls /var/lib/dumpsgen/interm-output-*gz )
echo -n | gzip > blot.gz
date
for filen in $filenames; do
    sort -t$'\t' -k2n -k3  -m <(zcat /var/lib/dumpsgen/blot.gz) <(zcat $filen) |  datamash -g 1,2 sum 3 | gzip > /var/lib/dumpsgen/blot2.gz
    mv /var/lib/dumpsgen/blot2.gz /var/lib/dumpsgen/blot.gz
done
date
mv /var/lib/dumpsgen/blot.gz /var/lib/dumpsgen/final-output.gz


    
