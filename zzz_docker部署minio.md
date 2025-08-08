查看卷        docker ps -a
查看卷        docker volume ls
删除容器      docker  rm -f           minio1       
删除卷        docker volume rm -f      aaa2_minio_data






sudo docker run -p 9000:9000 -p 9001:9001 --name minio1 \
-e "MINIO_ROOT_USER=minioadmin" \
-e "MINIO_ROOT_PASSWORD=minioadmin" \
-e "MINIO_CORS_ALLOW_ORIGIN=*" \
-e "MINIO_CORS_ALLOW_METHOD=GET,PUT,POST,DELETE,HEAD" \
-e "MINIO_CORS_ALLOW_HEADER=*" \
-v ./data:/data \
-v ./config:/root/.minio \
-d minio/minio server /data --console-address ":9001"



安装MinIO--Client
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/



mc alias set myminio http://103.119.2.223:9000 minioadmin minioadmin


参考文章
express上传                           https://juejin.cn/post/7390335741627629578
nestjs-上传文件到磁盘与minio却别       https://juejin.cn/post/7274518244085710905
前端MinIO上传                         https://blog.csdn.net/Allen_kaihui/article/details/140096218