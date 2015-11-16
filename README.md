# upyun_cdn
又拍云cdn上传

### 使用方法

upyun_cdn(options, auth, [callback])

options:
* `src` glob pattern
* `dest` upload path

auth:
* `bucket`
* `operator`
* `password`

callback:
* 参数为error

### 示例
```js
upyun_cdn({
    src: 'build/**/*.js',
    dest: '/build'
}, {
    bucket: 'upyun-text',
    operator: 'test',
    password: 'test'
}, function(err) {
    if (err) {
        console.error(err);
    }
});
```
