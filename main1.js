// [新代码ai生产] 分片上传功能 - 服务器端预签名URL生成
const express = require('express');
const Minio = require('minio');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = 3001;

// 中间件配置
app.use(cors());
app.use(express.json());
app.use(express.static('html'));

// MinIO客户端配置
const minioClient = new Minio.Client({
    endPoint: '103.119.2.223',
    port: 9000,
    useSSL: false,
    accessKey: 'minioadmin',
    secretKey: 'minioadmin'
});

// [新代码ai生产] 修复分片上传初始化 - 修正MinIO API调用参数
app.post('/api/initiate-multipart-upload', async (req, res) => {
    try {
        const { fileName, fileSize, chunkSize = 5 * 1024 * 1024 } = req.body; // 默认5MB分片
        
        if (!fileName || !fileSize) {
            return res.status(400).json({ error: '文件名和文件大小是必需的' });
        }

        // 生成唯一的上传ID
        const uploadId = uuidv4();
        const objectName = `uploads/${uploadId}/${fileName}`;
        
        // 计算分片数量
        const totalChunks = Math.ceil(fileSize / chunkSize);
        
        // 修复：正确调用MinIO分片上传初始化
        const uploadIdResult = await minioClient.initiateNewMultipartUpload('mybucket', objectName, {
            'Content-Type': 'application/octet-stream'
        });
        
        // 生成所有分片的预签名URL
        const presignedUrls = [];
        for (let partNumber = 1; partNumber <= totalChunks; partNumber++) {
            const presignedUrl = await minioClient.presignedPutObject(
                'mybucket', 
                `${objectName}_part_${partNumber}`, 
                24 * 60 * 60 // 24小时有效期
            );
            presignedUrls.push({
                partNumber,
                url: presignedUrl,
                startByte: (partNumber - 1) * chunkSize,
                endByte: Math.min(partNumber * chunkSize - 1, fileSize - 1)
            });
        }

        res.json({
            uploadId: uploadIdResult,
            objectName,
            totalChunks,
            chunkSize,
            presignedUrls,
            message: '分片上传初始化成功'
        });
        
    } catch (error) {
        console.error('初始化分片上传失败:', error);
        res.status(500).json({ error: '初始化分片上传失败', details: error.message });
    }
});

// [新代码ai生产] 修复完成分片上传 - 使用更简单的合并方式
app.post('/api/complete-multipart-upload', async (req, res) => {
    try {
        const { uploadId, objectName, parts } = req.body;
        console.log(req.body);
        
        if (!uploadId || !objectName || !parts || !Array.isArray(parts)) {
            return res.status(400).json({ error: '参数不完整' });
        }

        // 验证所有分片都已上传
        const uploadedParts = [];
        for (const part of parts) {
            try {
                const stat = await minioClient.statObject('mybucket', `${objectName}_part_${part.partNumber}`);
                uploadedParts.push({
                    partNumber: part.partNumber,
                    etag: stat.etag
                });
            } catch (error) {
                return res.status(400).json({ 
                    error: `分片 ${part.partNumber} 未找到或上传失败` 
                });
            }
        }

        // 修复：使用更简单的方式合并文件
        const finalObjectName = objectName.replace('_part_1', '').replace('_part_2', '').replace('_part_3', '');
        
        // 读取所有分片并合并
        console.log("uploadedParts---",uploadedParts);
        const chunks = [];
        for (const part of uploadedParts) {
            const chunkStream = await minioClient.getObject('mybucket', `${objectName}_part_${part.partNumber}`);
            const chunkBuffer = await streamToBuffer(chunkStream);
            chunks.push(chunkBuffer);
        }
        
        // 合并所有分片
        const finalBuffer = Buffer.concat(chunks);
        
        // 上传合并后的文件
        await minioClient.putObject('mybucket', finalObjectName, finalBuffer, finalBuffer.length, {
            'Content-Type': 'application/octet-stream'
        });

        // 清理分片文件
        for (const part of uploadedParts) {
            try {
                await minioClient.removeObject('mybucket', `${objectName}_part_${part.partNumber}`);
            } catch (error) {
                console.warn(`清理分片文件失败: ${objectName}_part_${part.partNumber}`, error);
            }
        }

        // 生成最终文件的下载URL
        const downloadUrl = await minioClient.presignedGetObject(
            'mybucket', 
            finalObjectName, 
            24 * 60 * 60 // 24小时有效期
        );

        res.json({
            success: true,
            finalObjectName,
            downloadUrl,
            message: '文件上传完成'
        });
        
    } catch (error) {
        console.error('完成分片上传失败:', error);
        res.status(500).json({ error: '完成分片上传失败', details: error.message });
    }
});

// [新代码ai生产] 获取文件下载URL
app.get('/api/download/:fileName', async (req, res) => {
    try {
        const { fileName } = req.params;
        const downloadUrl = await minioClient.presignedGetObject(
            'mybucket', 
            fileName, 
            24 * 60 * 60 // 24小时有效期
        );
        
        res.json({ downloadUrl });
    } catch (error) {
        console.error('生成下载URL失败:', error);
        res.status(500).json({ error: '生成下载URL失败' });
    }
});

// [新代码ai生产] 获取已上传文件列表
app.get('/api/files', async (req, res) => {
    try {
        const files = [];
        const stream = minioClient.listObjects('mybucket', 'uploads/', true);
        
        stream.on('data', (obj) => {
            if (!obj.name.includes('_part_')) { // 排除分片文件
                files.push({
                    name: obj.name,
                    size: obj.size,
                    lastModified: obj.lastModified
                });
            }
        });
        
        stream.on('end', () => {
            res.json({ files });
        });
        
        stream.on('error', (error) => {
            console.error('获取文件列表失败:', error);
            res.status(500).json({ error: '获取文件列表失败' });
        });
        
    } catch (error) {
        console.error('获取文件列表失败:', error);
        res.status(500).json({ error: '获取文件列表失败' });
    }
});

// [新代码ai生产] 辅助函数 - 将流转换为Buffer
function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('end', () => resolve(Buffer.concat(chunks)));
        stream.on('error', reject);
    });
}

// 启动服务器
app.listen(PORT, () => {
    console.log(`服务器运行在端口 ${PORT}`);
    console.log(`访问 http://localhost:${PORT} 查看上传界面`);
});
