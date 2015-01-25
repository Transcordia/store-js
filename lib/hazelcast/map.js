var log = require('ringo/logging').getLogger(module.id);
var {format} = java.lang.Format;

var elastic = require('../elasticsearch/search');
var {generateId} = require('./uuid');

var hazelcast = Packages.com.hazelcast.core.Hazelcast;

exports.Map = Map;

// An executor service will be created as a singleton and be available to all maps. This will
// allow us to manage the thread pool and cap it at a reasonable number of threads.
// todo: Expose the thread count as a configurable option
var executor = module.singleton(module.id, function() {
    var MAX_THREADS = 20;
    return new Packages.com.transcordia.platform.AsyncExecutorService(MAX_THREADS);
});

function Map(index, type) {

    // Constants -------------------------------------------------------------------

    const ID_PROP = '_id';

    // Instances -------------------------------------------------------------------

    // Keep track of the entry listeners added by the user.
    var _entryListeners = [];

	// Privates --------------------------------------------------------------------

    /**
     * Adds an entry listener which can be notified via callback function when an entry is added,
     * removed, updated or evicted from the map. These notifications will occur asynchronously and
     * they occur _after_ the event has been performed on the cache.
     *
     * The listener will have to provide at least one valid callback function. The function names
     * are entryAdded(entry), entryRemoved(entry), entryUpdated(entry) and entryEvicted(entry). If
     * the entry is a JSON object (which they all are), the entry will be a parsed JSON object. If
     * not, the entry parameter will be passed as-is.
     *
     * Each listener has a name property that uniquely identifies the listener. This is a bit of
     * a hack to prevent multiple listeners from being registered when the JS platform reloads
     * modules when they have been modified. In these cases, the registerListener() functions
     * fire again when the module is reloaded, and there is insufficient uniqueness in the
     * current set of parameters to determine if the registration is a duplicate or two
     * different listeners on the same event.
     *
     * @param listener
     */
    var addEntryListener = function(listener) {
        if (!listener.entryAdded && !listener.entryRemoved &&
                !listener.entryUpdated && !listener.entryEvicted) {
            throw 'EntryListener must implement a valid callback function.';
        }

        if (!listener.name) {
            throw 'EntryListener must have a unique name.';
        }

        // Remove a listener with the same name if it exists
        _entryListeners = _entryListeners.filter(function (l) {
            return listener.name !== l.name
        });

        _entryListeners.push(listener);
    };

    var clearEntryListeners = function() {
        _entryListeners = [];
    };

    var invokeListeners = function(eventName, event) {
        log.debug('Received internal event: {}, checking for listeners among [{}] candidates.',
                eventName, _entryListeners.length);
        if (_entryListeners.length > 0) {
            // Create the object we will be passing into each of our listeners
            var entry = {
                key: event.key,
                index: index,
                type: type,
                source: name,
                value: event.value,
                oldValue: event.oldValue
            };

            if (typeof entry.value === 'string') {
                try {
                    entry.value = JSON.parse(entry.value);
                } catch(e) {
                }
            }

            if (typeof entry.oldValue === 'string') {
                try {
                    entry.oldValue = JSON.parse(entry.oldValue);
                } catch(e) {
                }
            }

            for (var i = 0; i < _entryListeners.length; i++) {
                var handler = _entryListeners[i][eventName];
                if (handler) {
                    executor.submit(createTask(eventName, handler, entry));
                }
            }
        }

        function createTask(eventName, func, entry) {
            return new java.lang.Runnable({
                run: function() {
                    try {
                        func(entry);
                    } catch (e) {
                        log.error('Error executing listener event: {}', eventName, e);
                    }
                }
            });
        }
    };

	/**
	 * Put can be called with or without a 'key' parameter. If the key parameter is not present, the
	 * JSON value object is queried for an '_id' property. If found, the ID_PROP is the key. If not
	 * found, a new UUID is generated and used as the key.
	 *
	 * This is completely arbitrary, but I am stipulating the key is a string and the value is an
	 * object. If we decide later to allow values to be a string, date, or something else, this
	 * logic will have to be rewritten.
	 *
	 * @params {string, object [,ttl, units]}
	 * @params {object [,ttl, units]}
	 *
	 */
	var put = function() {
        if (arguments.length === 0)
            throw 'IllegalArgumentException, map.put requires at least one argument.';

        var args = Array.prototype.slice.call(arguments);

        var result = putValue.apply(this, args);

        if (typeof result === 'string') {
            return JSON.parse(result);
        } else {
            return null;
        }
	};


	var putValue = function() {
		var args = Array.prototype.slice.call(arguments);
		var types = args.map(function(arg) {
			return typeof arg;
		});

		// If the first parameter is a string, it is our key
		if (types[0] === 'string') {
			return putKeyValue.apply(this, args);
		}

		// If the first parameter is an object, we have to check to see if it has a key. If so, we
		// will use it, otherwise we will generate a new key.
		if (types[0] === 'object') {
			var value = args[0];
			var key = value[ID_PROP];
			if (!key) key = value[ID_PROP] = generateId();

			// push the key into the argument list and invoke putKeyValue
			args.unshift(key);
			return putKeyValue.apply(this, args);
		}

		throw 'IllegalArgumentException, map.put requires the first parameter to be a string or an ' +
				'object.';
	};


	/**
	 * Puts an entry into this map with a given ttl (time to live) value.
	 * Entry will expire and get evicted after the ttl.
	 *
	 * @param key {String} key of the entry
	 * @param value {Object} value of the entry
	 * @param ttl {Number} maximum time for this entry to stay in the map
	 * @param timeunit {String} time unit for the ttl
	 * @return {Object} old value of the entry
	 */
	var putKeyValue = function(key, value, ttl, timeunit) {
		log.debug('Putting into map <' + name + '>: ' + key);

        if(!ttl) ttl = 0;
        if(!timeunit) timeunit = null;
        if (typeof timeunit === 'string') timeunit = java.util.concurrent.TimeUnit.valueOf(timeunit);

		delete value._type;
		delete value._version;
		delete value._index;
		delete value._score;

        var json = JSON.stringify(value);
		return map.put(key, json, ttl, timeunit);
	};

	/**
	 * Returns the value to which the specified key is mapped,
	 * or {@code null} if this map contains no mapping for the key.
	 *
	 * <p>If this map permits null values, then a return value of
	 * {@code null} does not <i>necessarily</i> indicate that the map
	 * contains no mapping for the key; it's also possible that the map
	 * explicitly maps the key to {@code null}.  The {@link #containsKey
	 * containsKey} operation may be used to distinguish these two cases.
	 *
	 * @param key the key whose associated value is to be returned
	 * @return the value to which the specified key is mapped, or
	 *         {@code null} if this map contains no mapping for the key
	 */
	var get = function(key) {
		if (typeof key === 'object') return queryGet(key);
		log.debug('Getting from map [{}]: {}', name, key);
		var value = map.get(key);
		if (typeof value === 'string') {
			return JSON.parse(value);
		} else {
			return null;
		}
	};


    /**
     * Returns a set view of the keys contained in this map.  The set is
     * backed by the map, so changes to the map are reflected in the set, and
     * vice-versa.  If the map is modified while an iteration over the set is
     * in progress (except through the iterator's own <tt>remove</tt>
     * operation), the results of the iteration are undefined.  The set
     * supports element removal, which removes the corresponding mapping from
     * the map, via the <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt> <tt>retainAll</tt>, and <tt>clear</tt> operations.
     * It does not support the add or <tt>addAll</tt> operations.
     *
     * @return a set view of the keys contained in this map.
     */
	var keySet = function() {
        return map.keySet();
	};


	var count = function(query) {
		return elastic.count(query, index, type);
	};


	var evict = function(key) {
		log.debug('Evicting from map <' + name + '>: ' + key);
		return map.evict(key);
	};


	//var refresh = function(query) {
    var refresh = function(query) {
		return elastic.refresh(index);
	};


	/**
	 * Removes the mapping for a key from this map if it is present
	 * (optional operation).   More formally, if this map contains a mapping
	 * from key <tt>k</tt> to value <tt>v</tt> such that
	 * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping
	 * is removed.  (The map can contain at most one such mapping.)
	 *
	 * <p>Returns the value to which this map previously associated the key,
	 * or <tt>null</tt> if the map contained no mapping for the key.
	 *
	 * <p>The map will not contain a mapping for the specified key once the
	 * call returns.
	 *
	 * @param key key whose mapping is to be removed from the map
	 * @return the previous value associated with <tt>key</tt>, or
	 *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
	 */
	var remove = function(key) {
		if (typeof key === 'object') return queryRemove(key);
		log.debug('Removing from map [{}] using key: {}', name, key);
		var previous = map.remove(key);
		return (previous) ? JSON.parse(previous) : null;
	};


	/**
	 * Removes all of the mappings from this map (optional operation).
	 * The map will be empty after this call returns.
	 */
	var clear = function() {
		map.clear();
	};

	var size = function() {
		return map.size();
	};



	/**
	 * Hazelcast's strength is not querying because it is possible for some data to not yet be in
	 * the in-memory cache. Because of this, querying is better suited to be performed against
	 * Elastic Search or some other backing store which has access to the full set of data.
	 *
	 * @param json Object that describes the ES query
	 */
	var queryGet = function(json) {
        //var splitArr = name.split("-");
		log.debug('Searching map [{}]: {}', name, JSON.stringify(json));
		return elastic.search(json, index, type);
	};

	/**
	 * The query used to remove items from the map will be an ElasticSearch query. We don't
	 * currently have a way to easily determine which id's are affected by the query, so we will
	 * evict all elements from the map.
	 *
	 * // todo: It may be feasible to run the query and get the ids of all elements affected,
	 * then evict them individually from the Hazelcase map by id,
	 *
	 * @param json
	 */
	var queryRemove = function(json) {
		log.debug('Removing from map [{}] using query.: {}', name, JSON.stringify(json));
		elastic.remove(json, index, type);
		map.clear();
	};


	var toString = function() {
		return 'Hazelcast Map [' + name + ']';
	};

	// Constructors ----------------------------------------------------------------

    if (name === 'undefined' || type === 'undefined' || name === null || type === null)
    {
	    throw 'Name of index and type of map must be set.';
    }

    // The name of the map in Hazelcast is a concatination of index and type.
    var name = index + '-' + type;

    // Use the Hazelcast instance to retrieve the map by name
   	var map = hazelcast.getMap(name);

    // attach a listener to the map to be notified of any events that occur on the map.
    var listener = new com.hazelcast.core.EntryListener({
        entryAdded: function(event) {
            invokeListeners('entryAdded', event);
	    },
        entryRemoved: function(event) {
            invokeListeners('entryRemoved', event);
	    },
        entryUpdated: function(event) {
            invokeListeners('entryUpdated', event);
	    },
        entryEvicted: function(event) {
            invokeListeners('entryEvicted', event);
	    }
    });

    // We use the local version of the entry listener because we usually only want one node in the
    // cluster to handle the event. If all of our nodes were notified when an event fired, we would
    // have to do some distributed gymnastics to ensure only one node handled the event. So, this
    // local event listening is very crucial, and provides excellent performance.
    log.debug('Adding Hazelcast event listener to map [{}]', name);
    map.addLocalEntryListener(listener);


	return {
        addEntryListener: addEntryListener,
        clearEntryListeners: clearEntryListeners,
		clear: clear,
		evict: evict,
		get: get,
		keySet: keySet,
		hzObject: map,
        index: index,
        type: type,
		name: name,
		put: put,
		count: count,
		remove: remove,
		refresh: refresh,
		size: size,
		toString: toString
	};
}
