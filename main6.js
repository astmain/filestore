// [新代码ai生产] 超高速上传功能 - 方案6终极优化版
const express = require("express");
const Minio = require("minio");
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");
const path = require("path");

const app = express();
const PORT = 3006;

// 中间件配置
app.use(cors());
app.use(express.json());
app.use(express.static("html"));

// MinIO客户端配置
const minioClient = new Minio.Client({
  endPoint: "103.119.2.223",
  port: 9000,
  useSSL: false,
  accessKey: "minioadmin",
  secretKey: "minioadmin",
});

// 访问界面
app.use(express.static("html"));
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "html", "index6.html"));
});

// [新代码ai生产] 生成并发上传的预签名URL
app.post("/api/generate-concurrent-urls", async (req, res) => {
  try {
    const { fileName, fileSize, chunkSize = 2 * 1024 * 1024, maxConcurrency = 6 } = req.body;

    if (!fileName || !fileSize) {
      return res.status(400).json({ error: "文件名和文件大小是必需的" });
    }

    // 生成唯一的上传ID和对象名
    const uploadId = uuidv4();
    const objectName = `uploads/${uploadId}/${fileName}`;

    // 计算分片数量
    const totalChunks = Math.ceil(fileSize / chunkSize);

    // [新代码ai生产] 优化：批量生成预签名URL，提高效率
    const presignedUrls = [];
    const urlPromises = [];

    for (let i = 1; i <= totalChunks; i++) {
      const urlPromise = minioClient.presignedPutObject(
        "mybucket",
        `${objectName}_part_${i}`,
        24 * 60 * 60 // 24小时有效期
      );
      urlPromises.push(urlPromise);
    }

    // 并发获取所有预签名URL
    const urls = await Promise.all(urlPromises);

    // 组装结果
    for (let i = 1; i <= totalChunks; i++) {
      presignedUrls.push({
        partNumber: i,
        url: urls[i - 1],
        startByte: (i - 1) * chunkSize,
        endByte: Math.min(i * chunkSize - 1, fileSize - 1),
        objectKey: `${objectName}_part_${i}`,
      });
    }

    // 返回结果
    const result = {
      uploadId,
      objectName,
      totalChunks,
      chunkSize,
      maxConcurrency,
      presignedUrls,
      message: "并发上传预签名URL生成成功",
    };
    
    console.log(`生成 ${totalChunks} 个分片的预签名URL，并发数: ${maxConcurrency}`);
    res.json(result);
  } catch (error) {
    console.error("生成并发上传预签名URL失败:", error);
    res.status(500).json({ 
      error: "生成并发上传预签名URL失败", 
      details: error.message 
    });
  }
});

// [新代码ai生产] 合并分片文件 - 修复版本
app.post("/api/merge-chunks", async (req, res) => {
  try {
    const { objectName, totalChunks, chunkSize, fileSize } = req.body;

    if (!objectName || !totalChunks) {
      return res.status(400).json({ error: "对象名和分片数量是必需的" });
    }

    console.log(`开始合并文件: ${objectName}, 分片数: ${totalChunks}`);

    // [新代码ai生产] 修复：使用正确的文件合并方法
    try {
      // 方案1: 使用MinIO的composeObject方法（修复版本）
      const sourceObjects = [];
      for (let i = 1; i <= totalChunks; i++) {
        sourceObjects.push({
          bucket: "mybucket",
          object: `${objectName}_part_${i}`
        });
      }
      
      // 使用composeObject合并所有分片
      await minioClient.composeObject("mybucket", objectName, sourceObjects);
      console.log("使用composeObject合并成功");
      
    } catch (composeError) {
      console.log("composeObject失败，使用备用方案...", composeError.message);
      
      // 方案2: 使用multipart upload（修复版本）
      try {
        // 创建multipart upload
        const uploadId = await minioClient.initiateNewMultipartUpload("mybucket", objectName);
        console.log("创建multipart upload:", uploadId);

        const parts = [];
        
        // 上传所有分片
        for (let i = 1; i <= totalChunks; i++) {
          const partKey = `${objectName}_part_${i}`;
          
          // 获取分片数据
          const partStream = await minioClient.getObject("mybucket", partKey);
          const chunks = [];
          
          partStream.on('data', (chunk) => {
            chunks.push(chunk);
          });
          
          await new Promise((resolve, reject) => {
            partStream.on('end', resolve);
            partStream.on('error', reject);
          });
          
          const partBuffer = Buffer.concat(chunks);
          
          // 上传分片到multipart upload（修复contentType）
          const partResult = await minioClient.putObject("mybucket", objectName, partBuffer, partBuffer.length, {
            'x-amz-part-number': i.toString(),
            'x-amz-upload-id': uploadId,
            'Content-Type': 'application/octet-stream'
          });
          
          parts.push({
            ETag: partResult.etag,
            PartNumber: i
          });
          
          console.log(`上传分片 ${i}/${totalChunks} 到multipart upload完成`);
        }
        
        // 完成multipart upload
        await minioClient.completeMultipartUpload("mybucket", objectName, uploadId, parts);
        console.log("multipart upload完成");
        
      } catch (multipartError) {
        console.log("multipart upload也失败，使用最终备用方案...", multipartError.message);
        
        // 方案3: 使用Node.js流合并（修复路径问题）
        const fs = require('fs');
        const os = require('os');
        const path = require('path');
        
        // 使用系统临时目录
        const tempDir = os.tmpdir();
        const tempFileName = objectName.replace(/[\/\\]/g, '_').replace(/:/g, '_');
        const tempFilePath = path.join(tempDir, tempFileName);
        
        console.log(`使用临时文件路径: ${tempFilePath}`);
        
        // 创建写入流
        const writeStream = fs.createWriteStream(tempFilePath);
        
        // 按顺序读取并写入所有分片
        for (let i = 1; i <= totalChunks; i++) {
          const partKey = `${objectName}_part_${i}`;
          console.log(`读取分片: ${partKey}`);
          
          const partStream = await minioClient.getObject("mybucket", partKey);
          
          await new Promise((resolve, reject) => {
            partStream.pipe(writeStream, { end: false });
            partStream.on('end', resolve);
            partStream.on('error', reject);
          });
          
          console.log(`合并分片 ${i}/${totalChunks} 到临时文件完成`);
        }
        
        writeStream.end();
        
        // 等待写入完成
        await new Promise((resolve, reject) => {
          writeStream.on('finish', resolve);
          writeStream.on('error', reject);
        });
        
        console.log(`临时文件写入完成，大小: ${fs.statSync(tempFilePath).size} bytes`);
        
        // 将合并后的文件上传到MinIO
        const finalBuffer = fs.readFileSync(tempFilePath);
        await minioClient.putObject("mybucket", objectName, finalBuffer, finalBuffer.length, {
          'Content-Type': 'application/octet-stream'
        });
        
        // 删除临时文件
        fs.unlinkSync(tempFilePath);
        
        console.log("使用文件流合并方案完成");
      }
    }

    // [新代码ai生产] 清理分片文件
    console.log("清理分片文件...");
    const cleanupPromises = [];
    for (let i = 1; i <= totalChunks; i++) {
      const partObject = `${objectName}_part_${i}`;
      cleanupPromises.push(
        minioClient.removeObject("mybucket", partObject).catch(error => {
          console.warn(`删除分片文件失败: ${partObject}`, error);
        })
      );
    }

    await Promise.all(cleanupPromises);

    console.log(`文件合并完成: ${objectName}`);
    res.json({ 
      success: true, 
      objectName,
      message: "文件合并成功" 
    });
  } catch (error) {
    console.error("合并分片失败:", error);
    res.status(500).json({ 
      error: "合并分片失败", 
      details: error.message 
    });
  }
});

// [新代码ai生产] 获取文件下载URL
app.get("/api/download/:fileName", async (req, res) => {
  try {
    const { fileName } = req.params;
    const downloadUrl = await minioClient.presignedGetObject(
      "mybucket",
      fileName,
      24 * 60 * 60 // 24小时有效期
    );

    res.json({ downloadUrl });
  } catch (error) {
    console.error("生成下载URL失败:", error);
    res.status(500).json({ error: "生成下载URL失败" });
  }
});

// [新代码ai生产] 获取上传统计信息
app.get("/api/upload-stats", async (req, res) => {
  try {
    // 获取存储桶统计信息
    const bucketStats = await minioClient.bucketExists("mybucket");
    
    res.json({
      bucketExists: bucketStats,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error("获取上传统计失败:", error);
    res.status(500).json({ error: "获取上传统计失败" });
  }
});

// 启动服务器
app.listen(PORT, () => {
  console.log(`⚡ 超高速上传服务器运行在端口 ${PORT}`);
  console.log(`访问 http://localhost:${PORT}     查看上传界面`);
  console.log(`访问 http://103.119.2.223:9000    查看minio界面`);
  console.log(` 优化特性:`);
  console.log(`   - 并发上传 (默认6个并发)`);
  console.log(`   - 小分片 (2MB)`);
  console.log(`   - 批量预签名URL生成`);
  console.log(`   - 多重备用合并方案`);
  console.log(`   - 自动清理临时文件`);
});


// 速度还是太慢了,继续优化代码
// ⏱️ 时间统计
// 开始时间: 2025/8/8 01:39:32

// 结束时间: 2025/8/8 01:40:44

// 总耗时: 1分11.38秒

// 平均速度: 354.47 KB/s

// 压缩节省时间: 约29.2%

// 并发提升速度: 并发数6，速度提升约3倍

// 📥 下载链接
// 文件名: aaa.pdf

// 文件大小: 24.71 MB