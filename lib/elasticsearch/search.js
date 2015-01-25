var log = require('ringo/logging').getLogger(module.id);
var {format} = java.lang.String;

exports.search = search;
exports.searchRaw = searchRaw;
exports.terms = terms;
exports.count = count;
exports.remove = remove;
exports.refresh = refresh;
exports.initIndex = initIndex;

var {settingsBuilder} = Packages.org.elasticsearch.common.settings.ImmutableSettings;
var {nodeBuilder} = Packages.org.elasticsearch.node.NodeBuilder;
var {countRequest, searchRequest, deleteByQueryRequest, refreshRequest, indicesStatusRequest, createIndexRequest} = Packages.org.elasticsearch.client.Requests;
var {matchAllQuery} = Packages.org.elasticsearch.index.query.xcontent.QueryBuilders;
var {searchSource} = Packages.org.elasticsearch.search.builder.SearchSourceBuilder;
var {TransportClient} = Packages.org.elasticsearch.client.transport;
var {InetSocketTransportAddress} = Packages.org.elasticsearch.common.transport;


// Used to keep track of which indices have been created already.
var establishedIndices = [];

var clientCache = module.singleton(module.id+'-searchcache', function(){ return {}});

var es = {
    host: 'localhost',
    port: 9310
};

/*
 * In Elastic Search, there is the concept of indices and types. The index is passed as a property
 * on the request object. Types are used to differentiate entities in our domain. For example,
 * profile, resource, service proviver, and ventures are examples of types.
 *
 * i.e. Index = Database
 *      Type = Table
 */
function getClient() {
	if (clientCache.client == null) {
        clientCache.client = getTransportClient();

        // Is client connected to cluster?
        var health = clientCache.client.admin().cluster().prepareHealth().execute().actionGet();
        log.debug(format('ES client node created for cluster %s, number of data nodes: %.0f, cluster health: %s',
                health.clusterName(), health.numberOfDataNodes(), health.status()));
	}

	return clientCache.client;
}

function getTransportClient() {
    log.info(format('Search is creating transport client to connect to ES local data node.'));
    log.debug(format('Connecting to cluster at [%s:%.0f]', es.host, es.port));

    var address = new InetSocketTransportAddress['(java.lang.String,int)'](es.host, parseInt(es.port));
    var client = new TransportClient['(org.elasticsearch.common.settings.Settings)'](settingsBuilder()
            .put("client.transport.sniff", false).build())
            .addTransportAddress(address);

    log.debug(format('Transport client is now connected to cluster at [%s:%.0f]',
            es.host, es.port));

	return client;
}

function getNodeClient() {
    log.debug(format('Search is creating node client to connect to ES local data node.'));
    var localNode = nodeBuilder().build();

    if (log.isInfoEnabled()) {
        log.info("Starting a Elastic Search non-data node with these settings:");
        var map = localNode.settings().getAsMap();
        var keys = new java.util.ArrayList(map.keySet());
        var keyArray = keys.toArray() || [];
        keyArray.sort();

        keyArray.forEach(function(key) {
            log.info("    " + key + " : " + getValue(map, key));
        });
    }

	return localNode.start().client();
}

function getValue(map, key) {
    if (/^cloud\.aws\.secret/.test(key)) return "<HIDDEN>";
    return map.get(key);
}



/*
 * todo: Determine how many items should be returned by queries, and provide paging support.
 * Perhaps the default should be reasonable for 80% of the queries? Come up with a set of parameters
 * for paged result sets; first fifty contains a <link rel="next" src="..."> to the second fifty;
 * the second fifty contains <link rel="next" src="..."> as well as <link rel="prev" src="..."> to
 * fetch the next set. Of course, we would use the json equiv. { link: { rel: 'next', src: '...' } }
 *
 * It would be desirable for the user to supply a different value for the number of records per
 * paged result set. But we need a way to hook this into the elastic search scrolling api?
 * It has the right idea. Every query is given a token, and this token can be used when asking for
 * pages of data. Maybe /scroll/:scrollId/prev  and /scroll/:scrollId/next?
 *
 * var query = map.get( {query: { 'name.last': 'Korben' } },
  *                      sort: [ { name.first }, 'age' ]);
 * get( hostpath + query.scrollId )
 *     returns {
 *         scrollId: <string> the unique identifier for this scrollset
 *         pagesize: <number> the number of records contained in each page
 *         count: <number> the number of records in the scrollset
 *         currentIndex <number> the current record pointer lies just before this number
 *         page: <array> contains at most one page of <object> types for each record on the page
 *         prev: format('/scroll/%s/prev', scrollId),
 *         next: format('/scroll/%s/next', scrollId)
 *     }
 *
 * This idea should probably live in the map or store class.
 */

/**
 * Performs a count against the Elastic Search indices.
 *
 * @param json The query to execute a count against. If null, the count is for all objects.
 * @param types Optional fields that allows specific types to be searched. Use a String comprised of
 *              type names separated with a comma. (i.e. "profiles,providers,ventures")
 */
function count(json, index, types) {
    initIndex(index);

	if (typeof types === 'undefined') types = null;
	if (typeof types === 'string') types = types.split(',');

	if (!json) json = { "match_all": { } };
	var source = JSON.stringify(json);

	var request = countRequest(index).types(types).query(source);
    var response = getClient().count(request).actionGet();

	return response.count() || 0;
}

/**
 * Creates an index if it does not yet exist.
 */
function initIndex(index) {
	if (!index) throw 'Index must be provided.';

    // Only need to initialize an index once
	if (establishedIndices.indexOf(index) >= 0) return;

    // Get a reference to the ElasticSearch cluster admin interface
    var cluster = getClient().admin().cluster();

    // Wait until the cluster status is at least YELLOW
    cluster.prepareHealth().setWaitForYellowStatus().execute().actionGet();
    var response = cluster.prepareState().execute().actionGet();

    // Check to see if the index exists
    var hasIndex = response.getState().metaData().hasIndex(index);

    // If no index is there, create one
    if (!hasIndex) {
        // Create the index
        log.debug(format('Attempting to create index %s.', index));

        getClient().admin().indices().create(createIndexRequest(index)).actionGet();
        log.debug(format('Index %s created, waiting for Yellow state', index));

        response = cluster.prepareHealth().setWaitForYellowStatus().execute().actionGet();
        log.debug(format('Index %s is in the %s state.',
                index, ['GREEN','YELLOW','RED'][response.getStatus().value()]));

        // Remember this work
        establishedIndices.push(index);
    } else {
        log.debug(format('Index %s already exists.', index));
    }
}

/**
 * Calls refresh on the index. When this call finishes execution, all of the pending documents
 * to be indexed will have been indexed.
 */
function refresh(index) {
    initIndex(index);

    //print("Refresh Index Here: " + index);
	var start = new Date().getTime();
    var result = getClient().admin().indices().
			refresh(refreshRequest(index)).actionGet();

	var elapsed = new Date().getTime() - start;
	log.debug('Elastic search refresh ended: ' + elapsed + ' ms., '
			+ ', failedShards: ' + result.failedShards());
}

/**
 * Performs a query against the Elastic Search indices.
 *
 * @param json Query object in json form
 * @param types Optional fields that allows specific types to be searched. Use a String comprised of
 *              type names separated with a comma. (i.e. "profiles,providers,ventures")
 */
function search(json, index, types) {
    initIndex(index);

	if (typeof types === 'undefined') types = null;
	if (typeof types === 'string') types = types.split(',');

	if (typeof json.size === 'undefined') json.size = 100;

	var source = JSON.stringify(json);
    log.debug(format('Performing searchRequest of indices: {%s}, types: (%s), source: %s',
			index, types ? types.toSource() : 'null', source));

	var request = searchRequest(index).source(source);
	if (types != null) request = request.types(types);


	var searchResponse = getClient().search(request).actionGet();
	var searchHits = searchResponse.hits().hits();

	return searchHits.map(function(hit) {
		var result = JSON.parse(hit.sourceAsString());
		result._type = hit.type();
		result._version = hit.version();
		result._index = hit.index();
		result._score = hit.score();
		return result;
	});
}

/**
 * Performs a query against the Elastic Search indices, but returns the full search result
 * (including facets).
 *
 * @param json Query object in json form
 * @param types Optional fields that allows specific types to be searched. Use a String comprised of
 *              type names separated with a comma. (i.e. "profiles,providers,ventures")
 */
function searchRaw(json, index, types) {
    initIndex(index);

	if (typeof types === 'undefined') types = null;
	if (typeof types === 'string') types = types.split(',');

    // If a size is not specified, default to 100
	if (typeof json.size === 'undefined') json.size = 100;

	var source = JSON.stringify(json);
    log.debug(format('Performing searchRequest of indices: {%s}, types: (%s), source: %s',
			index, types ? types.toSource() : 'null', source));

	var request = searchRequest(index).source(source);
	if (types != null) request = request.types(types);


	var searchResponse = getClient().search(request).actionGet();

	return JSON.parse(searchResponse.toString());
}

/**
 * Returns the result of a term query against Elastic Search. A 'term" is a word that is seachable
 * in the indices.
 *
 * The json object returned is an array (possibly empty) of all the search terms represented as:
 *
 * [ { term: "name of term", freq: number of times term appears in index }, { ... }, etc. ]
 *
 * @param fields Array of fields for which to return terms. May be a string array, or a string with
 * elements separated by commas. '_all' will use all fields and is the default.
 * @param size Number of results to return.
 */
function terms(fields, size, index) {
    initIndex(index);

	if (typeof fields === 'undefined') fields = null;
	if (typeof fields === 'string') fields = fields.split(',');

	if (typeof size === 'undefined') size = 50;
    log.debug(format('Performing termRequest of indices: %s, fields: %s',
			index, fields ? fields.toSource() : 'null'));

	var request = termsRequest(index).sortType('freq');
	if (fields != null) request = request.fields(fields);
	if (!isNaN(size)) request = request.size(size);

	var termsResponse = getClient().terms(request).actionGet();

	// Our result includes an array of terms for each field indexed, but we will want to return the
	// list of terms across all fields, and we don't want any dupes.
	var map = {}, termFreq;

	for each (var fieldTermsFreq in termsResponse.fields()) {
		for each (termFreq in fieldTermsFreq.termsFreqs()) {
			var key = termFreq.termAsString();
			var termStore = map[key];
			if (!termStore) {
				termStore = {term: key, freq: termFreq.docFreq()};
				map[key] = termStore;
			} else {
				termStore.freq = termStore.freq + termFreq.docFreq();
			}
		}
	}

	var result = [];
	for each (termFreq in map) {
		log.debug('Term: ' + termFreq.term + ', frequency: ' + termFreq.freq);
		result.push(termFreq);
	}

	log.debug("Returning these search terms: " + JSON.stringify(result));

	return result;
}

/**
 * Removes the items from Elastic Search that match the query.
 *
 * @param json An Elastic Search query object in json format.
 * @param types Optional fields that allows specific types to be searched. Use a String comprised of
 *              type names separated with a comma. (i.e. "profiles,providers,ventures")
 */
function remove(json, index, types) {
    initIndex(index);

	if (typeof types === 'undefined') types = null;
	if (typeof types === 'string') types = types.split(',');

	var source = JSON.stringify(json);

	log.debug(format('Performing removal of indices: {%s}, types: (%s), source: %s',
			index, JSON.stringify(types), source));

	var request = deleteByQueryRequest(index).types(types).query(source);
	getClient().deleteByQuery(request).actionGet();
}
