function countScan(_redis, cursor, pattern, size, callback) {
    _redis.scan([cursor, 'MATCH', pattern, 'COUNT', 10000], (err, res) => {
        if (err) {
            callback(err, null);
        } else {
            cursor = res[0];
            size += res[1].length;

            if (cursor === '0') {
                callback(null, size);
            } else {
                countScan(_redis, cursor, pattern, size, callback);
            }
        }
    });
}

function delScan(_redis, cursor, pattern, size, callback) {
    _redis.scan([cursor, 'MATCH', pattern, 'COUNT', 10000], (err, res) => {
        if (err) {
            callback(err, null);
        } else {
            cursor = res[0];
            size += res[1].length;

            for (const key of res[1]) {
                _redis.del(key);
            }

            if (cursor === '0') {
                callback(null, size);
            } else {
                delScan(_redis, cursor, pattern, size, callback);
            }
        }
    });
}

exports.countScan = countScan;
exports.delScan = delScan;