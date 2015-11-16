var Upyun = require('upyun');
var mime = require('mime');
var path = require('path');
var through = require('through2');
var fs = require('vinyl-fs');
var gutil = require('gulp-util');
var colors = gutil.colors;
var Q = require('q');

var needUploadNum = 0; // 需要上传数量
var alreadyUploadNum = 0; // 已上传数量
var modifyFilesNum = 0; // 已上传，但本地修改数量
var errorCheckNum = 0; // 错误处理数量

var upyun;
var highWaterMark = 40960;
var logCheckDefer = Q.defer();
var uploadDefer = Q.defer();
var logUploadFailDefer = Q.defer();
var upyunErrorMsgMap = {
    '40000006': '上传前后MD5值不一样，上传不完整！',
    '40100006': '用户不存在！'
};

var errors = [];

function upyun_cdn(upload, auth, callback) {
    upyun = new Upyun(auth.bucket, auth.operator, auth.password, 'v1');
    return fs.src(upload.src, {read: false})
        .pipe(init(upload)) // 初始化属性值

        .pipe(checkRemoteFile()) // 是否存在文件
        .pipe(checkRemoteFile()) // retry
        .pipe(checkRemoteFile()) // retry
        .on('end', function() {
            console.log();
            logCheckDefer.resolve();
        })
        .pipe(logCheckFailed())
        .on('end', function() {
            uploadDefer.resolve();
        })

        .pipe(uploadFile()) // 上传文件
        .pipe(uploadFile()) // retry
        .pipe(uploadFile()) // retry
        .on('end', function() {
            logUploadFailDefer.resolve();
        })
        .pipe(logUploadFail())
        on('end', function() {
            if (errors.length) {
                callback(errors.join(','));
            }
        });
}

function init(upload) {
    return through.obj({highWaterMark: highWaterMark}, function(file, encoding, next) {
        var cdnpath = util.getCdnPath(file, upload);

        file.checkTryCount = 0;
        file.uploadTryCount = 0;
        file.cdnPath = cdnpath;
        file.needCheck = true;
        file.needUpload = false;
        file.needCompare = false;

        next(null, file);
    });
}

function checkRemoteFile() {
    return through.obj({highWaterMark: highWaterMark}, function(file, encoding, next) {
        if (file.needCheck) {
            upyun.existsFile(file.cdnPath, function(error, result) {
                if (error) {
                    errorCheckNum++;

                    file.needCheck = true;
                    file.checkFailMsg = '网络出错！';
                    file.checkFailRes = JSON.stringify(error);
                    file.checkTryCount++;
                } else {
                    var status = +result.statusCode;
                    if (status === 200) {
                        // 如果本地大小与服务器上的大小不一样，则认为本地修改了文件
                        if (file.stat.size == result.data.size) {
                            alreadyUploadNum++;
                        } else {
                            modifyFilesNum++;
                        }
                        file.needCheck = false;
                    } else if (status === 404) {
                        needUploadNum++;

                        file.needCheck = false;
                        file.needUpload = true;
                    } else {
                        errorCheckNum++;

                        file.needCheck = true;
                        file.checkTryCount++;
                        file.checkFailRes = JSON.stringify(result);
                        file.checkFailMsg = upyunErrorMsgMap[result.headers['x-error-code']];
                    }
                }

                // 重试错误计算
                if (file.checkTryCount > 1) {
                    errorCheckNum--;
                }

                next(null, file);
                util.logCheck();
            });
        } else {
            next(null, file);
        }
    });
}

function logCheckFailed() {
    return through.obj({highWaterMark: highWaterMark}, function(file, encoding, next) {
        if (file.needCheck) {
            util.logCheckFail(file);
        }
        next(null, file);
    });
}

function uploadFile() {
    return through.obj({highWaterMark: highWaterMark}, function(file, encoding, next) {
        if (file.needUpload) {
            upyun.uploadFile(file.cdnPath,
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
                    } else {
                        var upyunErrorCode = result.headers['x-error-code'];

                        file.uploadFailRes = JSON.stringify(result);
                        file.uploadFailMsg = upyunErrorMsgMap[upyunErrorCode] || upyunErrorCode;
                        file.uploadTryCount++;
                    }
                }

                next(null, file);
                util.logUpload(file);
            });
        } else {
            next(null, file);
        }
    });
}

function logUploadFail() {
    return through.obj({highWaterMark: highWaterMark}, function(file, encoding, next) {
        if (file.needUpload) {
            util.logUploadFail(file);
        }
        next(null, file);
    });
}

var util = {
    getCdnPath: function(file, upload) {
        var cdnpath = path.relative(file.base, file.path);
        cdnpath = path.join('/', upload.dest, cdnpath);
        return cdnpath;
    },

    logCheck: function() {
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
        process.stdout.write('相同: ' + alreadyUploadNum + '\t\t' +
                             '需要上传: ' + needUploadNum + '\t\t' +
                             '错误:' + errorCheckNum);
    },

    logCheckFail: function(file) {
        logCheckDefer.promise.then(function() {
            gutil.log(colors.red('检查对比'),
                      colors.red(file.path), '→', colors.red(file.cdnPath), '\n',
                      colors.red('失败原因：'), colors.red(file.checkFailMsg), '\n',
                      colors.red('返回信息：'), colors.red(file.checkFailRes));
            errors.push(file.checkFailRes);
        });
    },

    logUpload: function(file) {
        uploadDefer.promise.then(function() {
            if (file.uploadSuccess) {
                gutil.log('上传又拍完毕', colors.green(file.path), '→', colors.green(file.cdnPath));
            } else {
                // gutil.log(colors.red('上传失败'), colors.red(file.path), '→', colors.red(file.cdnPath));
                // gutil.log(colors.red('失败原因：'), colors.red(file.uploadFailMsg),
                //           colors.red('返回信息：'), colors.red(file.uploadFailRes));
            }
        });
    },

    logUploadFail: function(file) {
        logUploadFailDefer.promise.then(function() {
            gutil.log(colors.red('上传又拍失败'),
                      colors.red(file.path), '→', colors.red(file.cdnPath), '\n',
                      colors.red('失败原因：'), colors.red(file.uploadFailMsg), '\n',
                      colors.red('返回信息：'), colors.red(file.uploadFailRes));
            errors.push(file.checkFailRes);
        });
    }
};

module.exports = upyun_cdn;
