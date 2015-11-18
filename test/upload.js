var upyun_cdn = require('../');
var should = require('should');
var path = require('path');

var auth = {bucket: 'travis', operator: 'travisci', password: 'testtest'};
var src = path.join(__dirname, 'c.txt');
var dest = '/' + Date.now() + '/';

describe('TEST UPYUN CDN UPLOAD: ', function() {

    describe('upload file', function() {
        it('upload one file', function(done) {
            upyun_cdn({src: src, dest: dest}, auth, function(err, result) {
                err.should.be.exactly('');
                result.needUploadNum.should.be.exactly(1);
                done();
            });
        });
    });

    describe('already upload check', function() {
        it('already has one file upload', function(done) {
            upyun_cdn({src: src, dest: dest}, auth, function(err, result) {
                err.should.be.exactly('');
                result.alreadyUploadNum.should.be.exactly(1);
                done();
            });
        });
    });
});