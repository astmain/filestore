// [新代码ai生产] 超高速上传功能 - 方案7极致优化版
const express = require("express");
const Minio = require("minio");
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");
const path = require("path");
const cluster = require("cluster");
const os = require("os");

const app = express();
const PORT = 3007;

// [新代码ai生产] 多进程优化：如果是主进程，启动工作进程
if (cluster.isMaster) {
  const numCPUs = os.cpus().length;
  console.log(`主进程启动，创建 ${numCPUs} 个工作进程...`);
  
  for (let i = 0; i < numCPUs; i++) {
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
app.use(express.json());
app.use(express.static("html"));

// [新代码ai生产] 优化MinIO客户端配置 - 连接池和超时优化
const minioClient = new Minio.Client({
  endPoint: "103.119.2.223",
  port: 9000,
  useSSL: false,
  accessKey: "minioadmin",
  secretKey: "minioadmin",
  // [新代码ai生产] 连接池优化
  maxRetries: 3,
  retryDelay: 1000,
  // [新代码ai生产] 超时优化
  requestTimeout: 30000,
  // [新代码ai生产] 并发连接数优化
  maxConnections: 100,
});

// 访问界面
app.use(express.static("html"));
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "html", "index7.html"));
});

// [新代码ai生产] 生成并发上传的预签名URL - 极致优化版
app.post("/api/generate-concurrent-urls", async (req, res) => {
  try {
    const { fileName, fileSize, chunkSize = 1 * 1024 * 1024, maxConcurrency = 12 } = req.body;

    if (!fileName || !fileSize) {
      return res.status(400).json({ error: "文件名和文件大小是必需的" });
    }

    // [新代码ai生产] 动态优化分片大小
    let optimizedChunkSize = chunkSize;
    if (fileSize > 100 * 1024 * 1024) { // 大于100MB
      optimizedChunkSize = 5 * 1024 * 1024; // 5MB分片
    } else if (fileSize > 50 * 1024 * 1024) { // 大于50MB
      optimizedChunkSize = 3 * 1024 * 1024; // 3MB分片
    } else if (fileSize < 10 * 1024 * 1024) { // 小于10MB
      optimizedChunkSize = 512 * 1024; // 512KB分片
    }

    // [新代码ai生产] 动态优化并发数
    let optimizedConcurrency = maxConcurrency;
    if (fileSize > 100 * 1024 * 1024) {
      optimizedConcurrency = Math.min(16, maxConcurrency);
    } else if (fileSize < 10 * 1024 * 1024) {
      optimizedConcurrency = Math.min(8, maxConcurrency);
    }

    // 生成唯一的上传ID和对象名
    const uploadId = uuidv4();
    const objectName = `uploads/${uploadId}/${fileName}`;

    // 计算分片数量
    const totalChunks = Math.ceil(fileSize / optimizedChunkSize);

    // [新代码ai生产] 批量生成预签名URL - 使用Promise.allSettled提高容错性
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
        reason: fileSize > 100 * 1024 * 1024 ? "大文件优化" : 
                fileSize > 50 * 1024 * 1024 ? "中等文件优化" : "小文件优化"
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

// [新代码ai生产] 合并分片文件 - 极致优化版
app.post("/api/merge-chunks", async (req, res) => {
  try {
    const { objectName, totalChunks, chunkSize, fileSize } = req.body;

    if (!objectName || !totalChunks) {
      return res.status(400).json({ error: "对象名和分片数量是必需的" });
    }

    console.log(`开始合并文件: ${objectName}, 分片数: ${totalChunks}`);

    // [新代码ai生产] 优化：使用更高效的合并策略
    try {
      // 方案1: 使用MinIO的composeObject方法（优化版本）
      const sourceObjects = [];
      for (let i = 1; i <= totalChunks; i++) {
        sourceObjects.push({
          bucket: "mybucket",
          object: `${objectName}_part_${i}`
        });
      }
      
      // [新代码ai生产] 分批合并，避免一次性处理太多对象
      if (sourceObjects.length <= 32) {
        // 直接合并
        await minioClient.composeObject("mybucket", objectName, sourceObjects);
        console.log("使用composeObject直接合并成功");
      } else {
        // 分批合并
        const batchSize = 32;
        let tempObjects = [];
        
        for (let i = 0; i < sourceObjects.length; i += batchSize) {
          const batch = sourceObjects.slice(i, i + batchSize);
          const tempObjectName = `${objectName}_temp_${Math.floor(i / batchSize)}`;
          
          await minioClient.composeObject("mybucket", tempObjectName, batch);
          tempObjects.push({
            bucket: "mybucket",
            object: tempObjectName
          });
          
          console.log(`合并批次 ${Math.floor(i / batchSize) + 1}/${Math.ceil(sourceObjects.length / batchSize)}`);
        }
        
        // 最终合并
        await minioClient.composeObject("mybucket", objectName, tempObjects);
        
        // 清理临时文件
        for (const tempObj of tempObjects) {
          await minioClient.removeObject("mybucket", tempObj.object).catch(() => {});
        }
        
        console.log("使用composeObject分批合并成功");
      }
      
    } catch (composeError) {
      console.log("composeObject失败，使用备用方案...", composeError.message);
      
      // 方案2: 使用multipart upload（优化版本）
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
            
            // 上传分片到multipart upload
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
  console.log(` 极致优化特性:`);
  console.log(`   - 多进程架构 (${os.cpus().length} 个CPU核心)`);
  console.log(`   - 动态分片大小 (根据文件大小自动调整)`);
  console.log(`   - 动态并发数 (根据文件大小自动调整)`);
  console.log(`   - 连接池优化 (最大100个并发连接)`);
  console.log(`   - 批量合并优化 (支持大文件分批合并)`);
  console.log(`   - 异步清理 (不阻塞响应)`);
  console.log(`   - 容错处理 (Promise.allSettled)`);
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

// 还是非常的慢,继续优化代码
// 下面是main7.js的日志
// 生成 25 个分片的预签名URL，并发数: 12，分片大小: 1MB
// 开始合并文件: uploads/c448f34a-3c78-4612-b8c1-bae696163b52/aaa.pdf, 分片数: 25
// composeObject失败，使用备用方案... sourceConfig should an array of CopySourceOptions 
// multipart upload也失败，使用最终备用方案... contentType should be of type "object"
// 使用临时文件路径: C:\Users\admin\AppData\Local\Temp\uploads_c448f34a-3c78-4612-b8c1-bae696163b52_aaa.pdf
// 合并分片 1/25 到临时文件完成
// 合并分片 2/25 到临时文件完成
// 合并分片 3/25 到临时文件完成
// 合并分片 4/25 到临时文件完成
// 合并分片 5/25 到临时文件完成
// 合并分片 6/25 到临时文件完成
// 合并分片 7/25 到临时文件完成
// 合并分片 8/25 到临时文件完成
// 合并分片 9/25 到临时文件完成
// 合并分片 10/25 到临时文件完成
// 合并分片 11/25 到临时文件完成
// 合并分片 12/25 到临时文件完成
// 合并分片 13/25 到临时文件完成
// 合并分片 14/25 到临时文件完成
// 合并分片 15/25 到临时文件完成
// 合并分片 16/25 到临时文件完成
// 合并分片 17/25 到临时文件完成
// 合并分片 18/25 到临时文件完成
// 合并分片 19/25 到临时文件完成
// 合并分片 20/25 到临时文件完成
// 合并分片 21/25 到临时文件完成
// 合并分片 22/25 到临时文件完成
// 合并分片 23/25 到临时文件完成
// 合并分片 24/25 到临时文件完成
// 合并分片 25/25 到临时文件完成
// 临时文件写入完成，大小: 25909207 bytes
// 使用文件流合并方案完成
// 文件合并完成: uploads/c448f34a-3c78-4612-b8c1-bae696163b52/aaa.pdf
// 开始异步清理分片文件...
// 分片文件清理完成