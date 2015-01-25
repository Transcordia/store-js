var log = require('ringo/logging').getLogger(module.id);
var assert = require("assert");

var {request} = require('ringo/httpclient');
var {baseUrl} = require('./integration');

exports.setUp = function() {
//    log.info('Setup');
};

exports.tearDown = function() {
//    log.info('tearDown');
};


// start the test runner if we're called directly from command line
if (require.main == module.id) {
    system.exit(require('test').run(exports));
}
