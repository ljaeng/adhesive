#!/bin/sh

rm -fr release
mkdir release

cd core/

mvn package -Dmaven.test.skip=true

mv target/adhesive-job-1.0-SNAPSHOT.jar ../release/

cd -