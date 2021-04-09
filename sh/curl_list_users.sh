#!/bin/bash
#curl -i -s -XGET -H "Authorization: Bearer `cat secret/lgryglicki.prod.token`" -s "`cat helm/da-affiliation/secrets/PLATFORM_USER_SERVICE_ENDPOINT.prod.secret`/users?pageSize=5000&offset=0" | jq '.'
curl -s -XGET -H "Authorization: Bearer `cat secret/lgryglicki.prod.token`" -s "`cat helm/da-affiliation/secrets/PLATFORM_USER_SERVICE_ENDPOINT.prod.secret`/users?pageSize=5000&offset=0" | jq '.' > out
curl -s -XGET -H "Authorization: Bearer `cat secret/lgryglicki.prod.token`" -s "`cat helm/da-affiliation/secrets/PLATFORM_USER_SERVICE_ENDPOINT.prod.secret`/users?pageSize=5000&offset=0" | jq -r '.Data[].Emails[].EmailAddress' | sort | uniq
