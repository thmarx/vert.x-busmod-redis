/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

load('test_utils.js')
load('vertx.js')

var tu = new TestUtils();

var eb = vertx.eventBus;


function testSet() {
  eb.send('test.persistor', {
    command: 'set',
    key : "test:name",
    value : "test"
  }, function(reply) {
    tu.azzert(reply.status === 'ok');
    tu.testComplete();
  });
}

function testSort(){
  eb.send('test.persistor', {
    command: 'sort',
    key: 'test:names',
    alpha: true,
    by: 'test:names:*',
    get: 'test:names:*',
    start: 0,
    count: 2,
    order: 'desc'
  }, function(reply){
    tu.azzert(reply.status === 'ok');
    tu.azzert(reply.value[0] === 'Ringo');
    tu.azzert(reply.value[1] === 'Paul');
    tu.azzert(reply.value.length === 2);
    tu.testComplete();
  });
}

function testStoredSort(){
  eb.send('test.persistor', {
    command: 'sort',
    key: 'test:names',
    alpha: true,
    by: 'test:names:*',
    get: 'test:names:*',
    start: 0,
    count: 2,
    order: 'desc',
    resultkey: 'test:names:sorted'
  }, function(reply){
    tu.azzert(reply.status === 'ok');
    tu.testComplete();
  });
}

tu.registerTests(this);
var persistorConfig = {address: 'test.persistor', db_name: 'test_db'}
vertx.deployModule('de.marx-labs.redis-client-v0.4', persistorConfig, 1, function() {
  load('test_data.js');
  tu.appReady();
});

function vertxStop() {
  tu.unregisterAll();
  tu.appStopped();
}
