var elasticsearch = require('./elasticsearch/search');

exports.search = elasticsearch.search;
exports.searchRaw = elasticsearch.searchRaw;
exports.terms = elasticsearch.terms;
exports.count = elasticsearch.count;
exports.remove = elasticsearch.remove;
exports.refresh = elasticsearch.refresh;
exports.initIndex = elasticsearch.initIndex;


var hazelcastStore = require('./hazelcast/store');

exports.getMap = hazelcastStore.getMap;
exports.lock = hazelcastStore.lock;
exports.inTransaction = hazelcastStore.inTransaction;
exports.registerListener = hazelcastStore.registerListener;
exports.clearListeners = hazelcastStore.clearListeners;
//exports.getSet = hazelcastStore.getSet;


exports.generateId = require('./hazelcast/uuid').generateId;
