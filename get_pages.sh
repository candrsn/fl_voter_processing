#!/bin/bash

set -e 
get_dir_files() {
    dt="$1"
    wdir="$3"
    dlist="${2}.html"
    dcmd="${2}.sh"

    pushd download/${dt} >/dev/null
    curl -o ${dlist} "https://flvoters.com/download/${dt}/${wdir}"
    
    awk '/./ { f=split($0, p, "<li><a href=\""); if ( f < 2 ) { next; } split(p[2], q, "\""); if ( q[1] ~ /\/$/ ) { print "## " q[1]; } else {print "if [ ! -r " q[1] " ]; then curl -k -L  -O \"https://flvoters.com/download/'${dt}'/'${wdir}'" q[1] "\"; fi"; } }' ${dlist} > ${dcmd}

    awk '/^## \/download\// { next; }
    /^## / { b=$2; gsub(/\//, "", b); print b; }' ${dcmd}
    
    bash ./${dcmd}

    popd >/dev/null
    
}


get_extract() {
    dt="$1"
    mkdir -p download/${dt}
    
    wdirs=`get_dir_files "$dt" "ref" ""`

    for w in $wdirs; do
        get_dir_files "$dt" "${w}" "${w}/" 
    done

}

get_extract 20171130

get_extract 20161231
get_extract 20161130

get_extract 20150531
get_extract 20170228
get_extract 20130531
get_extract 20140531
