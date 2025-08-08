# 帮我写分片上传的功能

## 原本旧的1方案(已经写好了,不用修改)
html/index1.html(使用vue2,axios)
main1.js的端口号3001
main1.js服务器预签名，然后把需要上传、下载的 url 签名好给客户端html/index.html
客户端拿着 url 直接对接 minio 上传下载

## 现在我想写新的方案2(不要帮我写2方案)
html/index2.html(使用vue2,axios)
main2.js的端口号3002
main2.js服务器预签名
html/index1.html,分片上传合并(不经过main2.js),然后省略写(已上传文件)


## 我的minio信息是
endPoint: '103.119.2.223',
port: 9000,
useSSL: false,
accessKey: 'minioadmin',
secretKey: 'minioadmin'
桶的名称mybucket
http://103.119.2.223:9000

