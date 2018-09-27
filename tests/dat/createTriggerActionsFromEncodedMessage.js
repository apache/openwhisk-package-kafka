// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements; and to You under the Apache License, Version 2.0.

var openwhisk = require('openwhisk');

function main(params) {
    console.log(JSON.stringify(params));
    var name = new Buffer(params.messages[0].value, 'base64').toString('ascii');
    var ow = openwhisk({ignore_certs: true});
    return ow.triggers.create({name: name});
}
