var Upyun = require('upyun');
var mime = require('mime');
var through = require('through2');
var fs = require('vinyl-fs');
var Q = require('q');
var through2Concurrent = require('through2-concurrent');
var util = require('./util');
var highWaterMark = 1024
var upyunErrorMsgMap = {
    '40000006': '上传前后MD5值不一样，上传不完整！',
    '40100006': '用户不存在！'
};

module.exports = function(upload, auth, callback) {
    var  context = {
        needUploadNum: 0, // 需要上传数量
        alreadyUploadNum: 0, // 已上传数量
        modifyFilesNum: 0, // 已上传，但本地修改数量
        errorCheckNum: 0, // 错误处理数量

        logCheckDefer: Q.defer(),
        uploadDefer: Q.defer(),
        logUploadFailDefer: Q.defer(),

        upyun: new Upyun(auth.bucket, auth.operator, auth.password, 'v1'),

        errors: []
    };

    return fs.src(upload.src, {read: false})
        .pipe(init(upload)) // 初始化属性值

        .pipe(checkRemoteFile(context)) // 是否存在文件
        .pipe(checkRemoteFile(context)) // retry
        .pipe(checkRemoteFile(context)) // retry
        .on('end', function() {
            console.log();
            context.logCheckDefer.resolve();
        })
        .pipe(logCheckFailed(context))
        .on('end', function() {
            context.uploadDefer.resolve();
        })

        .pipe(uploadFile(context)) // 上传文件
        .pipe(uploadFile(context)) // retry
        .pipe(uploadFile(context)) // retry
        .on('end', function() {
            context.logUploadFailDefer.resolve();
        })
        .pipe(logUploadFail(context))
        .on('end', function() {
            callback && callback(context.errors.join(','), context);
        })
        // the end;
        .pipe(through.obj());
};

function init(upload) {
    return through2Concurrent.obj({highWaterMark: highWaterMark}, function(file, encoding, next) {
        var cdnpath = util.getCdnPath(file, upload);

        file.checkTryCount = 0;
        file.uploadTryCount = 0;
        file.cdnPath = cdnpath;
        file.needCheck = file.stat.isFile();
        file.needUpload = false;
        file.needCompare = false;

        next(null, file);
    });
}

function checkRemoteFile(context) {
    return through2Concurrent.obj({highWaterMark: highWaterMark}, function(file, encoding, next) {
        if (file.needCheck) {
            var handleError = function(msg, res) {
                file.needCheck = true;
                file.checkTryCount++;
                file.checkFailMsg = msg;
                file.checkFailRes = res;

                if (file.checkTryCount > 0) {
                    context.errorCheckNum++;
                }
            }

            context.upyun.existsFile(file.cdnPath, function(error, result) {
                if (error) {
                    handleError('网络出错！', JSON.stringify(error));
                } else {
                    var status = +result.statusCode;
                    if (status === 200) {
                        // 如果本地大小与服务器上的大小不一样，则认为本地修改了文件
                        if (file.stat.size == result.data.size) {
                            context.alreadyUploadNum++;
                        } else {
                            context.modifyFilesNum++;
                        }
                        file.needCheck = false;

                        if (file.checkTryCount > 0) {
                            context.errorCheckNum--;
                        }
                    } else if (status === 404) {
                        context.needUploadNum++;

                        file.needCheck = false;
                        file.needUpload = true;

                        if (file.checkTryCount > 0) {
                            context.errorCheckNum--;
                        }
                    } else {
                        handleError(upyunErrorMsgMap[result.headers['x-error-code']], JSON.stringify(result))
                    }
                }

                next(null, file);
                util.logCheck(context.alreadyUploadNum + context.modifyFilesNum, context.needUploadNum, context.errorCheckNum);
            });
        } else {
            next(null, file);
        }
    });
}

function logCheckFailed(context) {
    return through2Concurrent.obj({highWaterMark: highWaterMark}, function(file, encoding, next) {
        if (file.needCheck) {
            context.logCheckDefer.promise.then(function() {
                util.logCheckFail(file);
                context.errors.push(file.checkFailRes);
            });
        }
        next(null, file);
    });
}

function uploadFile(context) {
    return through2Concurrent.obj({highWaterMark: highWaterMark}, function(file, encoding, next) {
        if (file.needUpload) {
            context.upyun.uploadFile(file.cdnPath,
                             file.path,
                             mime.lookup(file.path),
                             true,  // important，检查上前后md5值是否一样
                             function(error, result) {
                file.uploadSuccess = false;
                if (error) {
                    file.uploadFailMsg = '网络出错！';
                    file.uploadFailRes = JSON.stringify(error);
                    file.uploadTryCount++;
                } else {
                    var status = +result.statusCode;
                    if (status === 200) {
                        file.uploadSuccess = true;
                        file.needUpload = false;

                        context.uploadDefer.promise.then(function() {
                            util.logUploadSuccess(file);
                        });
                    } else {
                        var upyunErrorCode = result.headers['x-error-code'];

                        file.uploadFailRes = JSON.stringify(result);
                        file.uploadFailMsg = upyunErrorMsgMap[upyunErrorCode] || upyunErrorCode;
                        file.uploadTryCount++;
                    }
                }

                next(null, file);
            });
        } else {
            next(null, file);
        }
    });
}

function logUploadFail(context) {
    return through2Concurrent.obj({highWaterMark: highWaterMark}, function(file, encoding, next) {
        if (file.needUpload) {
            context.logUploadFailDefer.promise.then(function() {
                util.logUploadFail(file);
                context.errors.push(file.uploadFailRes);
            });
        }
        next(null, file);
    });
}
