#!/bin/sh

mvn clean install -Dmaven.test.skip=true

rm -fr release
mkdir release

cd core/

mvn package -Dmaven.test.skip=true

mv target/adhesive-job-1.0-SNAPSHOT.jar ../release/

cd -