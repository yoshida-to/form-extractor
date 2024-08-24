const { app } = require('@azure/functions');
const { Readable } = require('stream');
const { createModelFromSchema, DocumentAnalysisClient, AzureKeyCredential } = require("@azure/ai-form-recognizer");
const { BlobServiceClient } = require('@azure/storage-blob');

const DOCUMENT_INTELLIGENCE_ENDPOINT = 'https://form-extractor-document-intelligence.cognitiveservices.azure.com/';
const OUTPUT_CONTAINER_NAME = 'output-file';

const PrebuiltLayoutModel = createModelFromSchema(
    {
        modelId: "prebuilt-layout",
        description: "Extract text and layout information from documents.",
        createdOn: "2023-02-28T00:00:00.000Z",
        apiVersion: "2023-07-31",
    }
);

// Buffer を Readable ストリームに変換する関数
function bufferToStream(buffer) {
    // Readable インスタンスを作成
    let stream = new Readable();
    
    // Buffer をストリームにプッシュ
    stream.push(buffer);
    
    // ストリームの終わりを示すために null をプッシュ
    stream.push(null);
    
    return stream;
}

app.storageBlob('BlobTrigger', {
    path: 'source-file/{name}',
    connection: 'formextractorstorage_STORAGE',
    handler: async (blob, context) => {
        const fileName = context.triggerMetadata.name;
        const DOCUMENT_INTELLIGENCE_KEY = process.env.DOCUMENT_INTELLIGENCE_KEY;
        const AZURE_STORAGE_CONNECTION_STRING = process.env.formextractorstorage_STORAGE;

        const readableStream = bufferToStream(blob);

        // document clientによる分析
        const documentAnalysisClient = new DocumentAnalysisClient(DOCUMENT_INTELLIGENCE_ENDPOINT, new AzureKeyCredential(DOCUMENT_INTELLIGENCE_KEY));
        const poller = await documentAnalysisClient.beginAnalyzeDocument(PrebuiltLayoutModel, readableStream, {
            onProgress: ({ status }) => {
                console.log(`status: ${status}`);
            },
        });

        const { tables } = await poller.pollUntilDone();

        const tableCsvContents = [];
        for (const table of tables || []) {
            // 行ごとにセルをグループ化した多重配列
            const rows = table.cells.reduce((rows, cell) => {
                if (!rows[cell.rowIndex]) {
                    rows[cell.rowIndex] = [];
                }
                rows[cell.rowIndex].push(cell);
                return rows;
            }, []);

            const csvContent = rows.map(row => row.map(cell => cell.content).join(',')).join('\n');
            tableCsvContents.push(csvContent);
        }

        context.log(tableCsvContents);

        const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
        const containerClient = blobServiceClient.getContainerClient(OUTPUT_CONTAINER_NAME);

        for (let i = 0; i < tableCsvContents.length; i++) {
            const tableNumber = i + 1;
            const blobName = `${fileName}_table${tableNumber}.csv`;
            const blockBlobClient = containerClient.getBlockBlobClient(blobName);
            await blockBlobClient.upload(tableCsvContents[i], tableCsvContents[i].length);
        }
    }
});
