function findMissingKeys(requiredKeys, testObject) {
    var missingKeys = [];

    requiredKeys.forEach(function(requiredKey) {
        if(!Object.keys(testObject).includes(requiredKey)) {
            missingKeys.push(requiredKey);
        }
    });

    return missingKeys;
}

exports.findMissingKeys = findMissingKeys;
