var uuid = Packages.java.util.UUID;

/**
 * Generates a UUID with or without dashes (removes dashes by default). Note, this creates a version
 * 4 UUID which is comprised of pseudo-random generated numbers. It might be better to resort to
 * using an implementation which takes the computers MAC address into consideration which is known
 * as a Type 1 UUID.
 *
 * @param leaveDashes
 */
exports.generateId = function(leaveDashes) {
    var result = uuid.randomUUID().toString();
    if (!leaveDashes) result = result.replace(/-/g, '');
    return result;
};

/**
 * Generates a base62 UUID. Saves 10 bytes.
 * @param leaveDashes
 */
function base62UUID() {
    var uuid = uuid.randomUUID();
    return encodeBase62(uuid.mostSignificantBits()) + '-' + encodeBase62(uuid.leastSignificantBits());
}



var digits = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');

var encodeBase62 = function(n) {
    var r = [], b = digits.length;

    while (n > 0) {
        var mod = n % b;
        r.push(digits[mod]);
        n = Math.floor(n / b);
    }

    return r.reverse().join('');
};

var decodeBase62 = function(s) {
    var n = 0,
            i = 0,
            b = digits.length,
            slen = s.length;

    for (; i < slen; i++) {
        n = (n * b) + digits.indexOf(s[i]);
    }
    return n;
};
