const { Worker } = require('bullmq');
const AzureDevOpsAgents = require('./azure-api');
const { QUEUE_NAME } = require('./queue-config');

// --- Worker Configuration ---
const CONCURRENCY = 15; // How many jobs to process in parallel
// ----------------------------

const azureApi = new AzureDevOpsAgents();

console.log(`🚀 Worker started. Ready to process jobs with a concurrency of ${CONCURRENCY}.`);

const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    const { organisation, project, buildId, patToken } = job.data;
    console.log(`Processing buildId: ${buildId}`);

    // This calls your original function to get the details for one build
    const result = await azureApi.getBuildAgentDetails(organisation, project, buildId, patToken);

    // *****************************************************************
    // This is where you would save the result to your database (upsert to PSQL)
    // For now, we'll just log it.
    console.log(`✅ Successfully processed buildId: ${buildId}`, result.data.computerName);
    // *****************************************************************

    return result;
  },
  {
    connection: { host: 'localhost', port: 6379 },
    concurrency: CONCURRENCY, // This is the key to parallel processing!
    removeOnComplete: { count: 1000 }, // Keep the last 1000 completed jobs
    removeOnFail: { count: 5000 },    // Keep the last 5000 failed jobs
  }
);

// --- Worker Event Listeners for better logging ---
worker.on('completed', (job, result) => {
  // console.log(`Job ${job.id} completed successfully. Result:`, result);
});

worker.on('failed', (job, err) => {
  console.error(`Job ${job.id} failed with error: ${err.message}`);
});

worker.on('error', err => {
  console.error('A worker error occurred:', err);
});
