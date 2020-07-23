#!/usr/bin/env bash
set -e
set -x

# Since we are using the system jruby, we need to make sure our jvm process
# uses at least 1g of memory, If we don't do this we can get OOM issues when
# installing gems. See https://github.com/elastic/logstash/issues/5179
export JRUBY_OPTS="-J-Xmx1g"
export GRADLE_OPTS="-Xmx4g -Dorg.gradle.daemon=false -Dorg.gradle.logging.level=info -Dfile.encoding=UTF-8"

SELECTED_TEST_SUITE=$1

# The acceptance test in our CI infrastructure doesn't clear the workspace between run
# this mean the lock of the Gemfile can be sticky from a previous run, before generating any package
# we will clear them out to make sure we use the latest version of theses files
# If we don't do this we will run into gem Conflict error.
[ -f Gemfile ] && rm Gemfile
[ -f Gemfile.lock ] && rm Gemfile.lock

LS_HOME="$PWD"
QA_DIR="$PWD/qa"

echo "Building Logstash artifacts"
cd $LS_HOME
rake artifact:all

echo "Acceptance: Installing dependencies"
cd $QA_DIR
bundle install

echo "Acceptance: Running the tests"
bundle exec rspec docker/spec

