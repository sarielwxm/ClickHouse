#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --ignore-error --query "DROP TABLE IF EXISTS tab_00651; CREATE TABLE tab_00651 (val UInt64) engine = Memory; SHOW CREATE TABLE tab_00651 format abcd; DESC tab_00651; DROP TABLE tab_00651;" 2>/dev/null ||:
