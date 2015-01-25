var log = require('ringo/logging').getLogger(module.id);
var {format} = java.lang.String;

var elastic = require('../elasticsearch/search');
var {generateId} = require('./uuid');

var hazelcast = Packages.com.hazelcast.core.Hazelcast;

exports.Set = Set;

function Set(name) {

	// Instances -------------------------------------------------------------------

	var ID_PROP = '_id';

	// Privates --------------------------------------------------------------------

	/**
	 * Put can be called with or without a 'key' parameter. If the key parameter is not present, the
	 * JSON value object is queried for an '_id' property. If found, the ID_PROP is the key. If not
	 * found, a new UUID is generated and used as the key.
	 *
	 * This is completely arbitrary, but I am stipulating the key is a string and the value is an
	 * object. If we decide later to allow values to be a string, date, or something else, this
	 * logic will have to be rewritten.
	 */
	var put = function() {
		if (arguments.length === 0)
			throw 'IllegalArgumentException, map.put requires at least one argument.';

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
		var json = JSON.stringify(value);
		return map.put(key, json, 0, null);
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

		var value = map.get(key);
		if (typeof value === 'string') {
			return JSON.parse(value);
		} else {
			return null;
		}
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
		var results = elastic.search(json);

		log.debug(format("Results from search (%s):\n\t{%s}",
				JSON.stringify(json), JSON.stringify(results)));

		return results;
	};

	var queryRemove = function(json) {

	};


	var toString = function() {
		return 'Hazelcast Map [' + name + ']';
	};

	// Constructors ----------------------------------------------------------------

	var set = hazelcast.getSet(name);

	return {
		clear: clear,
		get: get,
		name: name,
		put: put,
		remove: remove,
		size: size,
		toString: toString
	};
}
