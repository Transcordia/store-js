var log = require('ringo/logging').getLogger(module.id);
var assert = require("assert");

var {request} = require('ringo/httpclient');
var {baseUrl} = require('./integration');

var store = require('../../../lib/main');

var fred = {
    name: 'fred',
    age: 32
};
var barney = {
    name: 'barney',
    age: 31
};


exports.setUp = function() {
};

exports.tearDown = function() {
};

exports.testGetMap = function() {
    var map = store.getMap('index1', 'type1');
    assert.isNotUndefined(map);
};

exports.testMapPut = function() {
    var map = store.getMap('index1', 'type1');

    // When data is put into a map, it should return the prior value or null if not found.
    var priorValue = map.put('123', fred);
    assert.isNull(priorValue);

    // Check for not null.
    var oldFred = map.put('123', barney);
    assert.isNotNull(oldFred);

    // See if it equals the original value.
    assert.equal(fred.name, oldFred.name);
    assert.equal(fred.age, oldFred.age);
};


exports.testMapGet = function() {
    var map = store.getMap('index1', 'type1');

    map.put('123', fred);

    var oldFred = map.get('123');
    assert.isNotNull(oldFred);

    assert.equal(fred.name, oldFred.name);
    assert.equal(fred.age, oldFred.age);

    var unknown = map.get('321');
    assert.isNull(unknown);
};


exports.testMapRemove = function() {
    var map = store.getMap('index1', 'type1');

    map.put('123', 'fred');
    map.put('321', 'barney');

    var oldFred = map.get('123');
    assert.isNotNull(oldFred);

    // evict it from the cache
    map.remove('123');

    // Make sure it is gone,
    oldFred = map.get('123');
    assert.isNull(oldFred);

    // Make sure the other is still there
    var oldBarney = map.get('321');
    assert.isNotNull(oldBarney);

    assert.equal(1, map.size());
};

exports.testMapEvict = function() {
    var map = store.getMap('index1', 'type1');

    map.put('123', 'fred');
    map.put('321', 'barney');

    var oldFred = map.get('123');
    assert.isNotNull(oldFred);

    // evict it from the cache
    map.evict('123');

    // Make sure it is gone,
    oldFred = map.get('123');
    assert.isNull(oldFred);

    // Make sure the other is still there
    var oldBarney = map.get('321');
    assert.isNotNull(oldBarney);

    assert.equal(1, map.size());
};

exports.testTimeToLive = function() {
    var map = store.getMap('index1', 'type1');

    // Fred is added with a 3 second TTL, while barney has a 10 second TTL
    map.put('123', 'fred', 3, 'SECONDS');
    map.put('321', 'barney', 10000, 'MILLISECONDS');

    // Make sure they are still there
    var oldFred = map.get('123');
    assert.isNotNull(oldFred);
    var oldBarney = map.get('321');
    assert.isNotNull(oldBarney);

    // Snooze for 5 seconds
    java.lang.Thread.sleep(5000);

    // Make sure fred is gone,
    oldFred = map.get('123');
    assert.isNull(oldFred);

    // Barney should remain
    oldBarney = map.get('321');
    assert.isNotNull(oldBarney);

    // Snooze for 6 more seconds
    java.lang.Thread.sleep(6000);

    // Barney should now be evicted
    oldBarney = map.get('321');
    assert.isNull(oldBarney);
};

exports.testPutWithoutId = function() {
    var map = store.getMap('index1', 'type1');

    // Make sure map is empty
    map.clear();
    assert.equal(0, map.size());

    // Fred is added without specifying an id
    var priorValue = map.put(fred);

    // Prior value should be null
    assert.isNull(priorValue);

    // Make sure something is there
    assert.equal(1, map.size());

    // Get a list of the keys
    var keys = map.keySet();
    assert.isNotNull(keys);
    assert.equal(1, keys.size());

    // Iterate over the single key and make sure it is fred and there is an '_id' property.
    var i = keys.iterator();
    while (i.hasNext()) {
        var key = i.next();
        log.info('Key: {}', key);
        var oldFred = map.get(key);
        assert.equal(fred.name,  oldFred.name);
        assert.equal(fred.age,  oldFred.age);
        assert.isNotUndefined(fred._id);
    }
};


// ---------------------------------------------------------------------
// Map Listener tests
// ---------------------------------------------------------------------

exports.testMapListenerNoCallbacks = function() {
    var map = store.getMap('index1', 'type2');

    try {
        map.addEntryListener({
            name: module.id,
            someBadlyNamedCallback: function() {
            }
        });
        assert.fail('Map listener with no callbacks should fail.');
    } catch(e) {
        // Expected this
    }
};

exports.testMapListenerAddEntry = function() {
    var map = store.getMap('index1', 'type3');

    var result = false;

    map.clearEntryListeners();
    map.addEntryListener({
        name: module.id,
        entryAdded: function(entry) {
            try {
                assert.equal(typeof entry, 'object');
                assert.equal(entry.key, '1');
                assert.equal(entry.source, 'index1-type3');
                assert.equal(entry.index, 'index1');
                assert.equal(entry.type, 'type3');
                assert.equal(typeof entry.value, 'object');
                assert.equal(entry.value.name, 'fred');
                assert.isNull(entry.oldValue);
                result = true;
            } catch(e) {
                log.info('Error: ', e);
            }
        }
    });

    map.put('1', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.isTrue(result);
    map.remove('1');
};

exports.testMapListenerRemoveEntry = function() {
    var map = store.getMap('index1', 'type4');

    var result = false;

    map.clearEntryListeners();
    map.addEntryListener({
        name: module.id,
        entryRemoved: function(entry) {
            try {
                assert.equal(typeof entry.value, 'object');
                assert.equal(entry.value.name, 'fred');
                assert.isNull(entry.oldValue);
                result = true;
            } catch(e) {
                log.info('Error: ', e);
            }
        }
    });

    map.put('1', {name: 'fred'});
    assert.isFalse(result);

    map.remove('1');
    java.lang.Thread.sleep(1000);
    assert.isTrue(result);
};

exports.testMapListenerUpdateEntry = function() {
    var map = store.getMap('index1', 'type5');

    var result = false;

    map.clearEntryListeners();
    map.addEntryListener({
        name: module.id,
        entryUpdated: function(entry) {
            try {
                assert.equal(typeof entry.value, 'object');
                assert.equal(entry.value.name, 'barney');
                assert.equal(typeof entry.oldValue, 'object');
                assert.equal(entry.oldValue.name, 'fred');
                result = true;
            } catch(e) {
                log.info('Error: ', e);
            }
        }
    });

    map.put('1', {name: 'fred'});
    assert.isFalse(result);

    map.put('1', {name: 'barney'});
    java.lang.Thread.sleep(1000);
    assert.isTrue(result);
    map.remove('1');
};

exports.testMapListenerEvictEntry = function() {
    var map = store.getMap('index1', 'type6');

    var result = false;

    map.clearEntryListeners();
    map.addEntryListener({
        name: module.id,
        entryEvicted: function(entry) {
            try {
                assert.equal(typeof entry.value, 'object');
                assert.equal(entry.value.name, 'fred');
                result = true;
            } catch(e) {
                log.info('Error: ', e);
            }
        }
    });

    map.put('1', {name: 'fred'});
    assert.isFalse(result);

    map.evict('1');
    java.lang.Thread.sleep(1000);
    assert.isTrue(result);
};

exports.testRegisterListener = function() {
    var result = 0;

    store.clearListeners();
    store.registerListener(module.id, 'index2', 'type1', 'entryAdded', function() {
        result++;
    });

    var map = store.getMap('index2', 'type1');

    assert.equal(result, 0);
    map.put('1', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 1);

    store.registerListener(module.id, 'index2', ['type1', 'type2'], 'entryUpdated', function() {
        result++;
    });

    map.put('1', {name: 'barney'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 2);
};


exports.testStoreRegMultIndex = function() {
    var result = 0;

    store.clearListeners();
    store.registerListener(module.id, ['index3','index4'], 'type1', 'entryAdded', function() {
        result++;
    });

    var map1 = store.getMap('index3', 'type1');
    map1.put('1', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 1);

    var map2 = store.getMap('index4', 'type1');
    map2.put('1', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 2);
};


exports.testStoreRegMultType = function() {
    var result = 0;

    store.clearListeners();
    store.registerListener(module.id, 'index4', ['type1','type2'], 'entryAdded', function() {
        result++;
    });

    var map1 = store.getMap('index4', 'type1');
    map1.put('testStoreRegMultType1', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 1);

    var map2 = store.getMap('index4', 'type2');
    map2.put('testStoreRegMultType2', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 2);
};


exports.testStoreRegMultis = function() {
    var result = 0;

    store.clearListeners();
    store.registerListener(module.id, ['index3','index4'], ['type1','type2'], 'entryAdded', function() {
        result++;
    });

    var map1 = store.getMap('index3', 'type1');
    map1.put('testStoreRegMultis1', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 1);

    var map2 = store.getMap('index3', 'type2');
    map2.put('testStoreRegMultis2', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 2);

    var map3 = store.getMap('index4', 'type1');
    map3.put('testStoreRegMultis3', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 3);

    var map4 = store.getMap('index4', 'type2');
    map4.put('testStoreRegMultis4', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 4);
};


exports.testStoreRegNullIndex = function() {
    var result = 0;

    store.clearListeners();
    store.registerListener(module.id, null, ['type1','type2'], 'entryAdded', function() {
        result++;
    });

    var map1 = store.getMap('index3', 'type1');
    map1.put('testStoreRegNullIndex1', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 1);

    var map2 = store.getMap('index3', 'type2');
    map2.put('testStoreRegNullIndex2', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 2);

    var map3 = store.getMap('index4', 'type1');
    map3.put('testStoreRegNullIndex3', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 3);

    var map4 = store.getMap('index4', 'type2');
    map4.put('testStoreRegNullIndex4', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 4);
};


exports.testStoreRegNullType = function() {
    var result = 0;

    store.clearListeners();
    store.registerListener(module.id, 'index3', null, 'entryAdded', function() {
        result++;
    });

    var map1 = store.getMap('index3', 'type1');
    map1.put('testStoreRegNullType1', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 1);

    var map2 = store.getMap('index3', 'type2');
    map2.put('testStoreRegNullType2', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 2);
};


exports.testStoreRegNulls = function() {
    var result = 0;

    store.clearListeners();
    store.registerListener(module.id, null, null, 'entryAdded', function() {
        result++;
    });

    var map1 = store.getMap('index3', 'type1');
    map1.put('testStoreRegNulls1', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 1);

    var map2 = store.getMap('index3', 'type2');
    map2.put('testStoreRegNulls2', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 2);

    var map3 = store.getMap('index4', 'type1');
    map3.put('testStoreRegNulls3', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 3);

    var map4 = store.getMap('index4', 'type2');
    map4.put('testStoreRegNulls4', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 4);
};


exports.testMapListenerDupeEntries = function() {
    var result = 0;

    store.clearListeners();
    store.registerListener('ABC', 'index5', 'type3', 'entryAdded', function() {
        result++;
    });

    var map = store.getMap('index5', 'type3');

    assert.equal(result, 0);
    map.put('testMapListenerDupeEntries1', {name: 'fred'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 1);

    // Same entries, but a different name. Should be a second listener added.
    store.registerListener('123', 'index5', 'type3', 'entryAdded', function() {
        result++;
    });

    result = 0;
    map.put('testMapListenerDupeEntries2', {name: 'barney'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 2);

    // This listener should replace the previous listener because the name is the same.
    store.registerListener('ABC', 'index5', 'type3', 'entryAdded', function() {
        result++;
    });

    result = 0;
    map.put('testMapListenerDupeEntries3', {name: 'wilma'});
    java.lang.Thread.sleep(1000);
    assert.equal(result, 2);
};



// start the test runner if we're called directly from command line
if (require.main == module.id) {
    system.exit(require('test').run(exports));
}
