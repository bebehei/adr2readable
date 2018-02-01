#!/bin/bash

set -eu
set -o pipefail

ADR=${1?Please give ADR file con command line}
BASE=${2:-csv/}

mkdir -p $BASE

mdb-tables -1 ${ADR} | while read table; do
	file="$BASE/$( tr 'äöüß' 'aous' <<< "${table}").csv"
	mdb-export ${ADR} "${table}" > "${file}"
done
