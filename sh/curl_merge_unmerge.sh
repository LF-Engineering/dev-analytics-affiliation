#!/bin/bash
export JWT_TOKEN="`cat secret/lgryglicki.token`"
#export TESTING_API=1
./sh/mariadb.sh
ar1="1"
if [ ! -z "$2" ]
then
  ar1="$2"
fi
ar2="1"
if [ ! -z "$3" ]
then
  ar2="$3"
fi
if [ "$1" = "1" ]
then
  ./sh/curl_put_merge_unique_identities.sh 'odpi/egeria' 16fe424acecf8d614d102fc0ece919a22200481d aaa8024197795de9b90676592772633c5cfcb35a "$ar1"
  ./sh/curl_put_move_identity.sh 'odpi/egeria' 16fe424acecf8d614d102fc0ece919a22200481d 16fe424acecf8d614d102fc0ece919a22200481d "$ar2"
fi
if [ "$1" = "2" ]
then
  ./sh/curl_put_merge_unique_identities.sh 'odpi/egeria' aaa8024197795de9b90676592772633c5cfcb35a 16fe424acecf8d614d102fc0ece919a22200481d "$ar1"
  ./sh/curl_put_move_identity.sh 'odpi/egeria' aaa8024197795de9b90676592772633c5cfcb35a aaa8024197795de9b90676592772633c5cfcb35a "$ar2"
fi
if [ "$1" = "3" ]
then
  ./sh/curl_put_merge_unique_identities.sh 'odpi/egeria' 16fe424acecf8d614d102fc0ece919a22200481d aaa8024197795de9b90676592772633c5cfcb35a "$ar1"
  ./sh/curl_put_move_identity.sh 'odpi/egeria' aaa8024197795de9b90676592772633c5cfcb35a aaa8024197795de9b90676592772633c5cfcb35a "$ar2"
fi
if [ "$1" = "4" ]
then
  ./sh/curl_put_merge_unique_identities.sh 'odpi/egeria' aaa8024197795de9b90676592772633c5cfcb35a 16fe424acecf8d614d102fc0ece919a22200481d "$ar1"
  ./sh/curl_put_move_identity.sh 'odpi/egeria' 16fe424acecf8d614d102fc0ece919a22200481d 16fe424acecf8d614d102fc0ece919a22200481d "$ar2"
fi
