const {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
} = require("@aws-sdk/client-s3");

const fs = require("fs");
const path = require("path");

class S3MultipartUploader {

    constructor(region = 'us-east-1', options = {}) {
        const { 
          accessKeyId, 
          secretAccessKey, 
          useTransferAcceleration 
        } = options;
    
        const clientConfig = {
          credentials: {
            accessKeyId: accessKeyId,
            secretAccessKey: secretAccessKey
          },
          region: region,
          useAccelerateEndpoint: useTransferAcceleration,
        };
    
        this.s3Client = new S3Client(clientConfig);
    }


  async retry(fn, args, retryCount = 0) {
    try {
      return await fn(...args);
    } catch (error) {
      if (retryCount < 3) {
        console.log(`Attempt ${retryCount + 1} failed. Retrying...`);
        await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, retryCount))); // Exponential backoff
        return this.retry(fn, args, retryCount + 1);
      }
      throw error;
    }
  }


 
  async uploadFile(filePath, bucketName) {
    try {
      // File details
      const fileSize = fs.statSync(filePath).size;
      const fileName = path.basename(filePath);
      
      // Multipart upload configuration
      const minPartSize = 5 * 1024 * 1024; // 5 MB minimum
      const maxPartSize = 100 * 1024 * 1024; // 100 MB maximum
      
      // Calculate optimal part size
      let partSize = Math.max(minPartSize, Math.ceil(fileSize / 10000));
      partSize = Math.min(partSize, maxPartSize);

      const numberOfParts = Math.ceil(fileSize / partSize);

      // Prepare upload parameters
      const uploadParams = {
        Bucket: bucketName,
        Key: fileName,
      };

      // Initiate multipart upload with retry
      const multipartUpload = await this.retry(
        this.s3Client.send.bind(this.s3Client),
        [new CreateMultipartUploadCommand(uploadParams)]
      );

      console.log(`Multipart upload initiated for ${fileName}`);

      // Upload parts
      const uploadPromises = [];
      const uploadedParts = [];

      for (let partNumber = 1; partNumber <= numberOfParts; partNumber++) {
        const start = (partNumber - 1) * partSize;
        const end = Math.min(start + partSize, fileSize);

        const uploadPromise = new Promise((resolve, reject) => {
          const readStream = fs.createReadStream(filePath, { start, end: end - 1 });
          
          // Upload part with retry mechanism
          this.retry(
            this.s3Client.send.bind(this.s3Client),
            [new UploadPartCommand({
              ...uploadParams,
              PartNumber: partNumber,
              UploadId: multipartUpload.UploadId,
              Body: readStream,
            })]
          )
          .then(uploadPartResult => {
            uploadedParts.push({
              ETag: uploadPartResult.ETag,
              PartNumber: partNumber
            });
            resolve();
          })
          .catch(reject);
        });

        uploadPromises.push(uploadPromise);
      }

      // Wait for all parts to upload
      await Promise.all(uploadPromises);

      // Sort parts to ensure correct order
      uploadedParts.sort((a, b) => a.PartNumber - b.PartNumber);

      // Complete multipart upload with retry
      const completeMultipartUploadResult = await this.retry(
        this.s3Client.send.bind(this.s3Client),
        [new CompleteMultipartUploadCommand({
          ...uploadParams,
          UploadId: multipartUpload.UploadId,
          MultipartUpload: { Parts: uploadedParts }
        })]
      );

      console.log('File uploaded successfully:', completeMultipartUploadResult.Location);
      return completeMultipartUploadResult;

    } catch (error) {
      console.error('Error during multipart upload:', error);
      throw error;
    }
  }

}

// Example usage
async function main() {
    const options = { 
        accessKeyId : "accessKeyId", 
        secretAccessKey : "secretAccessKey", 
        useTransferAcceleration : true 
      } 

  // Create uploader with transfer acceleration
  const uploader = new S3MultipartUploader('eu-central-1', options);
  await uploader.uploadFile(
        "TestFile.pdf", // Local file path
        "bucket" // S3 Bucket Name
    );
}

main();
