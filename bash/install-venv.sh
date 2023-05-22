#!/bin/bash
#
# installs dbt-core virtual environments for use by cloud
#
# NOTE: this script is meant to be run within a dockerfile build.
#
# NOTE: an appropriate GITHUB_TOKEN must be defined in the environment.

VENV_ROOT=/venv

set -eo pipefail

TAG_NAME="latest"
# alternatively, specify an actual tag in order to pin the versions being used
# TAG_NAME=tags/<tag number here>

RELEASE=$(curl -s -H "Accept: application/vnd.github+json" -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/dbt-labs/venv-cache/releases/${TAG_NAME})
echo ${RELEASE}

# This step is just to capture any failures which might happen below,
# which would otherwise silently fail via the process substitution
# used at the bottom of the while loop:
# https://www.gnu.org/software/bash/manual/html_node/Process-Substitution.html
# In the event of a problem, we want a failure to be loud here,
# causing CI to fail.
if ! echo ${RELEASE} | jq -r ".assets[] | [.url, .name] | @tsv"; then
    echo "The GITHUB_TOKEN environment variable is probably missing or invalid."
    exit 1
fi

while IFS=$'\t' read -r uri name; do
    echo "retrieving name: ${name} uri: ${uri}"
    fullname=${VENV_ROOT}/${name}
    # tell curl to fail on any 400 status or above
    if ! curl --fail -H "Accept: application/octet-stream" -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "X-GitHub-Api-Version: 2022-11-28" -o ${fullname} -L ${uri}; then
        echo "Failed to curl asset: $?: ${uri}"
        exit 1
    fi
    extension="${fullname##*.}"
    if [ "${extension}" == "sqsh" ]; then
        dirname=${fullname%.*}
        unsquashfs -d ${dirname} ${fullname}
        rm -f ${fullname}
    fi
done < <(echo ${RELEASE} | jq -r ".assets[] | [.url, .name] | @tsv")
