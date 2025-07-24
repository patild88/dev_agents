const { Queue } = require('bullmq');

const QUEUE_NAME = 'azure-devops-agent-details';

// Create a new queue instance. It connects to Redis.
const agentDetailsQueue = new Queue(QUEUE_NAME, {
  connection: {
    host: 'localhost',
    port: 6379,
  },
  defaultJobOptions: {
    attempts: 3, // Retry a failed job up to 3 times
    backoff: {
      type: 'exponential',
      delay: 1000, // Wait 1s before the first retry, 2s for the second, etc.
    },
  },
});

module.exports = { agentDetailsQueue, QUEUE_NAME };
