const AzureDevOpsAgents = require('./azure-api');
const { agentDetailsQueue } = require('./queue-config');

// --- Your Configuration ---
const ORGANISATION = 'your-azure-devops-organisation';
const PROJECT = 'your-project-name';
const PIPELINE_ID = 123; // The numeric ID of your pipeline
const PAT_TOKEN = 'your-personal-access-token';
const BUILDS_TO_FETCH = 100; // How many recent builds to process
// --------------------------

const azureApi = new AzureDevOpsAgents();

async function addBuildsToQueue() {
  try {
    console.log('Starting producer...');
    const builds = await azureApi.getBuildsForPipeline(ORGANISATION, PROJECT, PIPELINE_ID, PAT_TOKEN, BUILDS_TO_FETCH);

    if (!builds || builds.length === 0) {
      console.log('No builds found for this pipeline.');
      return;
    }

    console.log(`Found ${builds.length} builds. Adding them to the queue...`);

    for (const build of builds) {
      const jobData = {
        organisation: ORGANISATION,
        project: PROJECT,
        buildId: build.id,
        patToken: PAT_TOKEN,
      };

      // Add a job to the queue. The job's name is 'fetch-agent-details'.
      // The job's ID is the build ID to prevent duplicates if you run this multiple times.
      await agentDetailsQueue.add('fetch-agent-details', jobData, {
        jobId: build.id
      });
    }

    console.log('All jobs have been added to the queue.');
  } catch (error) {
    console.error('Producer error:', error.response?.data || error.message);
  } finally {
    // Close the queue connection as the producer's job is done.
    await agentDetailsQueue.close();
  }
}

addBuildsToQueue();
