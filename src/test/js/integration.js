var system = require( 'system' );
require.paths.push(module.resolve('../lib'));

exports.baseUrl = 'http://localhost:8080/myapp/api';

var ClassPathXmlApplicationContext = Packages.org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Bootstrap Spring testing context
 */
var appContext;
module.singleton( 'io.ejs.store-js::integration-test', function () {
    appContext = new ClassPathXmlApplicationContext('spring-context.xml');
    appContext.start();
} );

function addTests(testModule) {
    for (var key in testModule) {
        exports[key] = testModule[key];
    }
}

addTests(require('./elasticsearch'));
addTests(require('./hazelcast'));


// start the test runner if we're called directly from command line
if (require.main == module.id) {
    var result = require('test').run(exports);
    appContext.close();
    system.exit(result);
}
