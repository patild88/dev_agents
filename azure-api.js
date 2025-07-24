const axios = require('axios');

class AzureDevOpsAgents {
  constructor() {
    this.baseUrl = 'https://dev.azure.com';
  }

  /**
   * Gets the list of builds for a specific pipeline definition.
   * This will be used by the producer to create jobs.
   */
  async getBuildsForPipeline(organisation, project, pipelineId, patToken, top = 50) {
    console.log(`Fetching latest ${top} builds for pipeline ${pipelineId}...`);
    const auth = Buffer.from(`:${patToken}`).toString('base64');
    const headers = { 'Authorization': `Basic ${auth}` };
    const buildsUrl = `${this.baseUrl}/${organisation}/${project}/_apis/build/builds?definitions=${pipelineId}&$top=${top}&api-version=7.0`;
    
    const response = await axios.get(buildsUrl, { headers });
    return response.data.value; // Returns the array of build objects
  }

  /**
   * Get agent details for a SINGLE build.
   * This is the core logic that our worker will execute for each job.
   */
  async getBuildAgentDetails(organisation, project, buildId, patToken) {
    try {
      const auth = Buffer.from(`:${patToken}`).toString('base64');
      const headers = { 'Authorization': `Basic ${auth}` };

      // Get build details to extract agent information
      const buildUrl = `${this.baseUrl}/${organisation}/${project}/_apis/build/builds/${buildId}?api-version=7.0`;
      const buildResponse = await axios.get(buildUrl, { headers });
      const build = buildResponse.data;

      if (!build.queue || !build.queue.pool) {
        throw new Error('No agent pool information found for this build');
      }

      const agentDetails = {
        buildId: buildId,
        buildNumber: build.buildNumber,
        agentPoolId: build.queue.pool.id,
        agentPoolName: build.queue.pool.name,
        agentId: null,
        agentName: null,
        computerName: null,
        status: build.status,
        result: build.result,
      };

      // Get the timeline to find which agent was used
      const timelineUrl = `${this.baseUrl}/${organisation}/${project}/_apis/build/builds/${buildId}/timeline?api-version=7.0`;
      const timelineResponse = await axios.get(timelineUrl, { headers });
      const agentJob = timelineResponse.data.records?.find(record => record.type === 'Job' && record.workerName);

      if (agentJob && agentJob.workerName) {
        agentDetails.agentName = agentJob.workerName;
        
        // Get more detailed agent information from the agent pool
        const agentPoolUrl = `${this.baseUrl}/${organisation}/_apis/distributedtask/pools/${build.queue.pool.id}/agents?api-version=7.0`;
        const agentPoolResponse = await axios.get(agentPoolUrl, { headers });
        const agent = agentPoolResponse.data.value?.find(a => a.name === agentJob.workerName);
        
        if (agent) {
          agentDetails.agentId = agent.id;
          agentDetails.computerName = agent.systemCapabilities?.['Agent.ComputerName'] || agent.systemCapabilities?.['System.HostName'] || agent.name;
        }
      }

      return { success: true, data: agentDetails };
    } catch (error) {
      console.error(`Error fetching details for build ${buildId}:`, error.message);
      // Re-throw the error so BullMQ knows the job failed and can retry it
      throw error;
    }
  }
}

module.exports = AzureDevOpsAgents;
