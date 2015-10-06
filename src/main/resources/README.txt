cat swdf.nt | iconv -f=ISO-8859-1 -t=UTF-8 | rapper -i ntriples -o ntriples - http://example.org/base/ | sort -u -S2G | pbzip2 > swdf.fixed.sorted.nt.bz2

cst-virt-load.sh swdf.nt.bz2 http://data.semanticweb.org/ 1111 dba dba

