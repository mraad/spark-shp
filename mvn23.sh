#!/usr/bin/env bash

nice mvn \
  -DskipTests=true \
  -Dgpg.skip=true \
  -Drat.skip=true \
  -Dmaven.javadoc.skip=true \
  -P spark-2.3 \
  -P esri-geometry-github \
  clean install
