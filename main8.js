// [新代码ai生产] 超高速上传功能 - 方案8激进优化版 - 修复预签名URL问题
const express = require("express");
const Minio = require("minio");
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");
const path = require("path");
const cluster = require("cluster");
const os = require("os");
const fs = require("fs");
const crypto = require("crypto");

const app = express();
const PORT = 3008;

// [新代码ai生产] 激进优化：如果是主进程，启动更多工作进程
if (cluster.isMaster) {
  const numCPUs = os.cpus().length;
  const workerCount = Math.max(numCPUs * 2, 8); // 激进：启动更多工作进程
  console.log(`主进程启动，创建 ${workerCount} 个工作进程...`);
  
  for (let i = 0; i < workerCount; i++) {
    cluster.fork();
  }
  
  cluster.on('exit', (worker, code, signal) => {
    console.log(`工作进程 ${worker.process.pid} 退出`);
    cluster.fork(); // 重启工作进程
  });
  
  return;
}

// 中间件配置
app.use(cors());
app.use(express.json({ limit: '100mb' })); // 增加请求体限制
app.use(express.static("html"));

// [新代码ai生产] 激进优化MinIO客户端配置
const minioClient = new Minio.Client({
  endPoint: "103.119.2.223",
  port: 9000,
  useSSL: false,
  accessKey: "minioadmin",
  secretKey: "minioadmin",
  // [新代码ai生产] 激进连接池优化
  maxRetries: 5,
  retryDelay: 500,
  // [新代码ai生产] 激进超时优化
  requestTimeout: 60000, // 1分钟超时
  // [新代码ai生产] 激进并发连接数
  maxConnections: 200,
});

// 访问界面
app.use(express.static("html"));
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "html", "index8.html"));
});

// [新代码ai生产] 激进优化：直接上传接口（跳过分片）
app.post("/api/direct-upload", async (req, res) => {
  try {
    const { fileName, fileData, fileSize } = req.body;

    if (!fileName || !fileData) {
      return res.status(400).json({ error: "文件名和文件数据是必需的" });
    }

    // 生成唯一对象名
    const uploadId = uuidv4();
    const objectName = `uploads/${uploadId}/${fileName}`;

    // 解码Base64数据
    const buffer = Buffer.from(fileData, 'base64');

    // 直接上传到MinIO
    await minioClient.putObject("mybucket", objectName, buffer, buffer.length, {
      'Content-Type': 'application/octet-stream'
    });

    console.log(`直接上传完成: ${objectName}, 大小: ${buffer.length} bytes`);

    res.json({
      success: true,
      objectName,
      message: "直接上传成功",
      uploadMethod: "direct"
    });
  } catch (error) {
    console.error("直接上传失败:", error);
    res.status(500).json({ 
      error: "直接上传失败", 
      details: error.message 
    });
  }
});

// [新代码ai生产] 激进优化：生成并发上传的预签名URL - 修复版本
app.post("/api/generate-concurrent-urls", async (req, res) => {
  try {
    const { fileName, fileSize, chunkSize = 5 * 1024 * 1024, maxConcurrency = 20 } = req.body;

    if (!fileName || !fileSize) {
      return res.status(400).json({ error: "文件名和文件大小是必需的" });
    }

    // [新代码ai生产] 激进优化分片策略
    let optimizedChunkSize = chunkSize;
    if (fileSize <= 10 * 1024 * 1024) { // 小于10MB，直接上传
      return res.json({
        shouldDirectUpload: true,
        message: "小文件建议直接上传"
      });
    } else if (fileSize <= 50 * 1024 * 1024) { // 10-50MB
      optimizedChunkSize = 10 * 1024 * 1024; // 10MB分片
    } else if (fileSize <= 200 * 1024 * 1024) { // 50-200MB
      optimizedChunkSize = 20 * 1024 * 1024; // 20MB分片
    } else { // 大于200MB
      optimizedChunkSize = 50 * 1024 * 1024; // 50MB分片
    }

    // [新代码ai生产] 激进优化并发数
    let optimizedConcurrency = maxConcurrency;
    if (fileSize <= 50 * 1024 * 1024) {
      optimizedConcurrency = Math.min(30, maxConcurrency);
    } else if (fileSize <= 200 * 1024 * 1024) {
      optimizedConcurrency = Math.min(25, maxConcurrency);
    } else {
      optimizedConcurrency = Math.min(20, maxConcurrency);
    }

    // 生成唯一的上传ID和对象名
    const uploadId = uuidv4();
    const objectName = `uploads/${uploadId}/${fileName}`;

    // 计算分片数量
    const totalChunks = Math.ceil(fileSize / optimizedChunkSize);

    // [新代码ai生产] 激进优化：批量生成预签名URL - 修复版本
    const urlPromises = [];
    for (let i = 1; i <= totalChunks; i++) {
      const urlPromise = minioClient.presignedPutObject(
        "mybucket",
        `${objectName}_part_${i}`,
        24 * 60 * 60 // 24小时有效期
      ).catch(error => {
        console.warn(`生成分片 ${i} 预签名URL失败:`, error);
        return null;
      });
      urlPromises.push(urlPromise);
    }

    // 并发获取所有预签名URL
    const urlResults = await Promise.allSettled(urlPromises);
    const presignedUrls = [];

    urlResults.forEach((result, index) => {
      if (result.status === 'fulfilled' && result.value) {
        const i = index + 1;
        presignedUrls.push({
          partNumber: i,
          url: result.value,
          startByte: (i - 1) * optimizedChunkSize,
          endByte: Math.min(i * optimizedChunkSize - 1, fileSize - 1),
          objectKey: `${objectName}_part_${i}`,
        });
      }
    });

    // 返回结果
    const result = {
      uploadId,
      objectName,
      totalChunks,
      chunkSize: optimizedChunkSize,
      maxConcurrency: optimizedConcurrency,
      presignedUrls,
      message: "并发上传预签名URL生成成功",
      optimizations: {
        originalChunkSize: chunkSize,
        optimizedChunkSize,
        originalConcurrency: maxConcurrency,
        optimizedConcurrency,
        reason: fileSize <= 50 * 1024 * 1024 ? "中等文件优化" : 
                fileSize <= 200 * 1024 * 1024 ? "大文件优化" : "超大文件优化"
      }
    };
    
    console.log(`生成 ${totalChunks} 个分片的预签名URL，并发数: ${optimizedConcurrency}，分片大小: ${optimizedChunkSize / 1024 / 1024}MB`);
    res.json(result);
  } catch (error) {
    console.error("生成并发上传预签名URL失败:", error);
    res.status(500).json({ 
      error: "生成并发上传预签名URL失败", 
      details: error.message 
    });
  }
});

// [新代码ai生产] 激进优化：合并分片文件 - 修复版本
app.post("/api/merge-chunks", async (req, res) => {
  try {
    const { objectName, totalChunks, chunkSize, fileSize } = req.body;

    if (!objectName || !totalChunks) {
      return res.status(400).json({ error: "对象名和分片数量是必需的" });
    }

    console.log(`开始合并文件: ${objectName}, 分片数: ${totalChunks}`);

    // [新代码ai生产] 激进优化：使用更高效的合并策略
    try {
      // 方案1: 使用MinIO的composeObject方法（修复版本）
      const sourceObjects = [];
      for (let i = 1; i <= totalChunks; i++) {
        sourceObjects.push({
          bucket: "mybucket",
          object: `${objectName}_part_${i}`
        });
      }
      
      // [新代码ai生产] 修复composeObject调用
      await minioClient.composeObject("mybucket", objectName, sourceObjects);
      console.log("使用composeObject合并成功");
      
    } catch (composeError) {
      console.log("composeObject失败，使用备用方案...", composeError.message);
      
      // 方案2: 使用multipart upload（修复版本）
      try {
        const uploadId = await minioClient.initiateNewMultipartUpload("mybucket", objectName);
        console.log("创建multipart upload:", uploadId);

        const parts = [];
        
        // [新代码ai生产] 并发处理分片
        const partPromises = [];
        for (let i = 1; i <= totalChunks; i++) {
          const partPromise = (async () => {
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
            
            return {
              ETag: partResult.etag,
              PartNumber: i
            };
          })();
          
          partPromises.push(partPromise);
        }
        
        // 等待所有分片处理完成
        const partResults = await Promise.all(partPromises);
        parts.push(...partResults);
        
        // 完成multipart upload
        await minioClient.completeMultipartUpload("mybucket", objectName, uploadId, parts);
        console.log("multipart upload完成");
        
      } catch (multipartError) {
        console.log("multipart upload也失败，使用最终备用方案...", multipartError.message);
        
        // 方案3: 使用Node.js流合并（优化版本）
        const os = require('os');
        const path = require('path');
        
        // 使用系统临时目录
        const tempDir = os.tmpdir();
        const tempFileName = objectName.replace(/[\/\\]/g, '_').replace(/:/g, '_');
        const tempFilePath = path.join(tempDir, tempFileName);
        
        console.log(`使用临时文件路径: ${tempFilePath}`);
        
        // 创建写入流
        const writeStream = fs.createWriteStream(tempFilePath);
        
        // [新代码ai生产] 并发读取分片，顺序写入
        const readPromises = [];
        for (let i = 1; i <= totalChunks; i++) {
          const partKey = `${objectName}_part_${i}`;
          const partNumber = i;
          
          const readPromise = (async () => {
            const partStream = await minioClient.getObject("mybucket", partKey);
            return { partNumber, partStream };
          })();
          
          readPromises.push(readPromise);
        }
        
        // 等待所有分片读取准备完成
        const partStreams = await Promise.all(readPromises);
        
        // 按顺序写入
        for (const { partNumber, partStream } of partStreams.sort((a, b) => a.partNumber - b.partNumber)) {
          await new Promise((resolve, reject) => {
            partStream.pipe(writeStream, { end: false });
            partStream.on('end', resolve);
            partStream.on('error', reject);
          });
          
          console.log(`合并分片 ${partNumber}/${totalChunks} 到临时文件完成`);
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

    // [新代码ai生产] 异步清理分片文件，不阻塞响应
    setImmediate(async () => {
      console.log("开始异步清理分片文件...");
      const cleanupPromises = [];
      for (let i = 1; i <= totalChunks; i++) {
        const partObject = `${objectName}_part_${i}`;
        cleanupPromises.push(
          minioClient.removeObject("mybucket", partObject).catch(error => {
            console.warn(`删除分片文件失败: ${partObject}`, error);
          })
        );
      }

      await Promise.allSettled(cleanupPromises);
      console.log("分片文件清理完成");
    });

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
      workerProcessId: process.pid,
      cpuCount: os.cpus().length,
      workerCount: cluster.isMaster ? os.cpus().length * 2 : 1,
    });
  } catch (error) {
    console.error("获取上传统计失败:", error);
    res.status(500).json({ error: "获取上传统计失败" });
  }
});

// [新代码ai生产] 健康检查接口
app.get("/api/health", (req, res) => {
  res.json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    workerProcessId: process.pid,
    memoryUsage: process.memoryUsage(),
    uptime: process.uptime(),
  });
});

// 启动服务器
app.listen(PORT, () => {
  console.log(`⚡ 超高速上传服务器运行在端口 ${PORT} (工作进程 ${process.pid})`);
  console.log(`访问 http://localhost:${PORT}     查看上传界面`);
  console.log(`访问 http://103.119.2.223:9000    查看minio界面`);
  console.log(` 激进优化特性:`);
  console.log(`   - 超多进程架构 (${os.cpus().length * 2} 个工作进程)`);
  console.log(`   - 激进分片策略 (10MB-50MB分片)`);
  console.log(`   - 激进并发数 (20-30个并发)`);
  console.log(`   - 直接上传模式 (小文件)`);
  console.log(`   - 超大连接池 (200个并发连接)`);
  console.log(`   - 修复合并算法 (composeObject修复)`);
  console.log(`   - 激进超时设置 (60秒)`);
});

// [新代码ai生产] 优雅关闭处理
process.on('SIGTERM', () => {
  console.log(`工作进程 ${process.pid} 收到SIGTERM信号，准备关闭...`);
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log(`工作进程 ${process.pid} 收到SIGINT信号，准备关闭...`);
  process.exit(0);
});

// [新代码ai生产] 激进优化：内存监控
setInterval(() => {
  const memUsage = process.memoryUsage();
  if (memUsage.heapUsed > 500 * 1024 * 1024) { // 超过500MB
    console.warn(`内存使用过高: ${Math.round(memUsage.heapUsed / 1024 / 1024)}MB`);
    global.gc && global.gc(); // 强制垃圾回收
  }
}, 30000); // 每30秒检查一次 