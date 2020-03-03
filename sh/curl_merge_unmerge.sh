#!/bin/bash
export JWT_TOKEN="`cat secret/lgryglicki.token`"
export TESTING_API=1
./sh/mariadb.sh
if [ ! "$1" = "alt" ]
then
  ./sh/curl_put_merge_unique_identities.sh 'odpi/egeria' 16fe424acecf8d614d102fc0ece919a22200481d aaa8024197795de9b90676592772633c5cfcb35a
  ./sh/curl_put_move_identity.sh 'odpi/egeria' 16fe424acecf8d614d102fc0ece919a22200481d 16fe424acecf8d614d102fc0ece919a22200481d
else
  ./sh/curl_put_merge_unique_identities.sh 'odpi/egeria' aaa8024197795de9b90676592772633c5cfcb35a 16fe424acecf8d614d102fc0ece919a22200481d
  ./sh/curl_put_move_identity.sh 'odpi/egeria' aaa8024197795de9b90676592772633c5cfcb35a aaa8024197795de9b90676592772633c5cfcb35a
fi
