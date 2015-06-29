#! /bin/bash
DATETIME=$(date '+%Y-%m-%d-%h-%m-%s')
/usr/local/bin/parallel --no-notice \
    --colsep '\s' \
    --resume \
    --results utf8-audit-rows-output-${DATETIME} \
    --joblog utf8-audit-rows-output-${DATETIME}/utf8-audit-${DATETIME}.joblog \
    --progress \
    --arg-file utf8-audit-rows-output-${DATETIME}/tables_to_fix.txt  \
    --verbose \
    ./utf8-audit.py --schema={1} --table={2}
