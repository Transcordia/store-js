require.paths.push(module.resolve('../lib'));

exports.baseUrl = 'http://localhost:8080/myapp/api';

function addTests(testModule) {
    for (var key in testModule) {
        exports[key] = testModule[key];
    }
}

addTests(require('./elasticsearch'));
addTests(require('./hazelcast'));


// start the test runner if we're called directly from command line
if (require.main == module.id) {
    system.exit(require('test').run(exports));
}
