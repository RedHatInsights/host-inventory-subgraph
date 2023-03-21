#!/bin/bash

set +e
podman run ${IMAGE}:${IMAGE_TAG} npm test
result=$?
set -e

# Evaluate the test result.

# If you set up a python virtual env for your tests, move back to the bonfire virtual env...
source .bonfire_venv/bin/activate

# If your unit tests store junit xml results, you should store them in a file matching format `artifacts/junit-*.xml`
# If you have no junit file, use the below code to create a 'dummy' result file so Jenkins will not fail
mkdir -p $ARTIFACTS_DIR
cat << EOF > $ARTIFACTS_DIR/junit-dummy.xml
<testsuite tests="1">
    <testcase classname="dummy" name="dummytest"/>
</testsuite>
EOF

if [ $result -ne 0 ]; then
    echo '====================================='
    echo '====  ✖ ERROR: UNIT TEST FAILED  ===='
    echo '====================================='
    exit 1
fi
