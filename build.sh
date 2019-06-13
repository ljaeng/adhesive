#!/bin/sh

rm -fr release
mkdir release

cd launcher/

mvn package -Dmaven.test.skip=true

cd -