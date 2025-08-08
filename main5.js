// [新代码ai生产] 分片上传功能 - 方案2服务器端预签名URL生成
const express = require("express");
const Minio = require("minio");
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");
const path = require("path"); // [新代码ai生产] 添加path模块导入，用于处理文件路径

const app = express();
const PORT = 3005;

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
  res.sendFile(path.join(__dirname, "html", "index5.html"));
});

// [新代码ai生产] 生成分片上传的预签名URL - 客户端直接对接MinIO
app.post("/api/generate-upload-urls", async (req, res) => {
  try {
    const { fileName, fileSize, chunkSize = 5 * 1024 * 1024 } = req.body; // 默认5MB分片

    if (!fileName || !fileSize) {
      return res.status(400).json({ error: "文件名和文件大小是必需的" });
    }

    // 生成唯一的上传ID和对象名
    const uploadId = uuidv4();
    const objectName = `uploads/${uploadId}/${fileName}`;

    // 计算分片数量
    const totalChunks = Math.ceil(fileSize / chunkSize);

    // 生成所有分片的预签名URL
    const presignedUrls = [];
    for (let partNumber = 1; partNumber <= totalChunks; partNumber++) {
      const presignedUrl = await minioClient.presignedPutObject(
        "mybucket",
        `${objectName}_part_${partNumber}`,
        24 * 60 * 60 // 24小时有效期
      );
      presignedUrls.push({
        partNumber,
        url: presignedUrl,
        startByte: (partNumber - 1) * chunkSize,
        endByte: Math.min(partNumber * chunkSize - 1, fileSize - 1),
        objectKey: `${objectName}_part_${partNumber}`,
      });
    }

    // 生成合并文件的预签名URL
    const mergePresignedUrl = await minioClient.presignedPutObject(
      "mybucket",
      objectName,
      24 * 60 * 60 // 24小时有效期
    );

    // 返回结果
    const result = {
      uploadId,
      objectName,
      totalChunks,
      chunkSize,
      presignedUrls,
      mergePresignedUrl,
      message: "预签名URL生成成功",
    };
    console.log("result---", result);
    res.json(result);
  } catch (error) {
    console.error("生成预签名URL失败:", error);
    res
      .status(500)
      .json({ error: "生成预签名URL失败", details: error.message });
  }
});

// [新代码ai生产] 新增：生成合并文件的预签名URL - 优化版本
app.post("/api/generate-merge-url", async (req, res) => {
  try {
    const { fileName, fileSize } = req.body;

    if (!fileName || !fileSize) {
      return res.status(400).json({ error: "文件名和文件大小是必需的" });
    }

    // 生成唯一的对象名
    const uploadId = uuidv4();
    const objectName = `uploads/${uploadId}/${fileName}`;

    // 生成合并文件的预签名URL
    const mergePresignedUrl = await minioClient.presignedPutObject(
      "mybucket",
      objectName,
      24 * 60 * 60 // 24小时有效期
    );

    // 返回结果
    const result = {
      objectName,
      mergePresignedUrl,
      message: "合并文件预签名URL生成成功",
    };
    console.log("merge-url-result---", result);
    res.json(result);
  } catch (error) {
    console.error("生成合并文件预签名URL失败:", error);
    res
      .status(500)
      .json({ error: "生成合并文件预签名URL失败", details: error.message });
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

// 启动服务器
app.listen(PORT, () => {
  console.log(`方案5服务器运行在端口 ${PORT}`);
  console.log(`访问 http://localhost:${PORT}     查看上传界面`);
  console.log(`访问 http://103.119.2.223:9000    查看minio界面`);
});

