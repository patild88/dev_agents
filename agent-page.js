package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// --- Structs for API Responses ---

type Project struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type ProjectsResponse struct {
	Value []Project `json:"value"`
}

type AgentPool struct {
	ID         int       `json:"id"`
	Name       string    `json:"name"`
	PoolType   string    `json:"poolType"`
	Size       int       `json:"size"`
	IsHosted   bool      `json:"isHosted"`
	CreatedOn  time.Time `json:"createdOn"`
}

type AgentPoolsResponse struct {
	Value []AgentPool `json:"value"`
}

type Agent struct {
	ID                   int                    `json:"id"`
	Name                 string                 `json:"name"`
	Version              string                 `json:"version"`
	Status               string                 `json:"status"`
	Enabled              bool                   `json:"enabled"`
	CreatedOn            time.Time              `json:"createdOn"`
	LastCompletedRequest time.Time              `json:"lastCompletedRequest"`
	SystemCapabilities   map[string]string      `json:"systemCapabilities"`
}

type AgentsResponse struct {
	Value []Agent `json:"value"`
}

type Build struct {
	ID         int    `json:"id"`
	BuildNumber string `json:"buildNumber"`
	Status     string `json:"status"`
	Result     string `json:"result"`
	StartTime  time.Time `json:"startTime"`
	FinishTime time.Time `json:"finishTime"`
	Queue      struct {
		Pool struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		} `json:"pool"`
	} `json:"queue"`
	Definition struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	} `json:"definition"`
}

type BuildsResponse struct {
	Value []Build `json:"value"`
}

type TimelineRecord struct {
	Type       string `json:"type"`
	WorkerName string `json:"workerName"`
}

type TimelineResponse struct {
	Records []TimelineRecord `json:"records"`
}

type LogFile struct {
	URL string `json:"url"`
}

type LogsResponse struct {
	Value []LogFile `json:"value"`
}

// AgentDetails holds the consolidated information for a build agent.
type AgentDetails struct {
	BuildID        int
	BuildNumber    string
	DefinitionID   int
	DefinitionName string
	AgentPoolID    int
	AgentPoolName  string
	AgentID        sql.NullInt64
	AgentName      sql.NullString
	ComputerName   sql.NullString
	StartTime      time.Time
	FinishTime     time.Time
	BuildStatus    string
	BuildResult    string
}

// AzureDevOpsAgentsSync handles the synchronization logic.
type AzureDevOpsAgentsSync struct {
	BaseURL    string
	HTTPClient *http.Client
	DB         *sql.DB
}

// NewAzureDevOpsAgentsSync creates a new sync client.
func NewAzureDevOpsAgentsSync() (*AzureDevOpsAgentsSync, error) {
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USERNAME")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &AzureDevOpsAgentsSync{
		BaseURL:    "https://dev.azure.com",
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
		DB:         db,
	}, nil
}

// InitializeSchema creates the necessary database tables and indexes.
func (s *AzureDevOpsAgentsSync) InitializeSchema() error {
	createTablesQuery := `
    CREATE TABLE IF NOT EXISTS azure_devops_agent_pools (
        id SERIAL PRIMARY KEY,
        organization VARCHAR(100) NOT NULL,
        pool_id INTEGER NOT NULL,
        pool_name VARCHAR(200) NOT NULL,
        pool_type VARCHAR(50),
        size INTEGER,
        is_hosted BOOLEAN DEFAULT FALSE,
        created_on TIMESTAMP,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(organization, pool_id)
    );
    CREATE TABLE IF NOT EXISTS azure_devops_agents (
        id SERIAL PRIMARY KEY,
        organization VARCHAR(100) NOT NULL,
        pool_id INTEGER NOT NULL,
        agent_id INTEGER NOT NULL,
        agent_name VARCHAR(200) NOT NULL,
        computer_name VARCHAR(200),
        version VARCHAR(50),
        status VARCHAR(50),
        enabled BOOLEAN DEFAULT TRUE,
        os_description VARCHAR(500),
        created_on TIMESTAMP,
        last_completed_request TIMESTAMP,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(organization, pool_id, agent_id)
    );
    CREATE TABLE IF NOT EXISTS azure_devops_build_agents (
        id SERIAL PRIMARY KEY,
        organization VARCHAR(100) NOT NULL,
        project VARCHAR(100) NOT NULL,
        build_id INTEGER NOT NULL,
        build_number VARCHAR(100),
        definition_id INTEGER,
        definition_name VARCHAR(200),
        agent_pool_id INTEGER,
        agent_pool_name VARCHAR(200),
        agent_id INTEGER,
        agent_name VARCHAR(200),
        computer_name VARCHAR(200),
        start_time TIMESTAMP,
        finish_time TIMESTAMP,
        build_status VARCHAR(50),
        build_result VARCHAR(50),
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(organization, project, build_id)
    );
    CREATE INDEX IF NOT EXISTS idx_agent_pools_org_pool ON azure_devops_agent_pools(organization, pool_id);
    CREATE INDEX IF NOT EXISTS idx_agents_org_pool ON azure_devops_agents(organization, pool_id);
    CREATE INDEX IF NOT EXISTS idx_build_agents_org_project ON azure_devops_build_agents(organization, project);
    CREATE INDEX IF NOT EXISTS idx_build_agents_build_id ON azure_devops_build_agents(build_id);
    CREATE INDEX IF NOT EXISTS idx_build_agents_definition_id ON azure_devops_build_agents(definition_id);
    `
	_, err := s.DB.Exec(createTablesQuery)
	if err != nil {
		return fmt.Errorf("failed to initialize database schema: %w", err)
	}
	log.Println("✅ Database schema initialized successfully")
	return nil
}

// --- API Helper ---
func (s *AzureDevOpsAgentsSync) doRequest(method, url, patToken string, target interface{}) (http.Header, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	auth := base64.StdEncoding.EncodeToString([]byte(":" + patToken))
	req.Header.Add("Authorization", "Basic "+auth)
	req.Header.Add("Content-Type", "application/json")

	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	if target != nil {
		if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return resp.Header, nil
}

// GetAllProjects fetches all projects in the organization.
func (s *AzureDevOpsAgentsSync) GetAllProjects(organisation, patToken string) ([]Project, error) {
	log.Printf("🔄 Fetching all projects for organization: %s", organisation)
	url := fmt.Sprintf("%s/%s/_apis/projects?api-version=7.0", s.BaseURL, organisation)
	var response ProjectsResponse
	_, err := s.doRequest("GET", url, patToken, &response)
	if err != nil {
		return nil, fmt.Errorf("error fetching projects: %w", err)
	}
	log.Printf("✅ Found %d projects in organization", len(response.Value))
	return response.Value, nil
}

// GetBuildAgentDetails fetches detailed agent information for a specific build.
func (s *AzureDevOpsAgentsSync) GetBuildAgentDetails(organisation, project, patToken string, buildID int) (*AgentDetails, error) {
	log.Printf("Fetching agent details for build %d in %s/%s", buildID, organisation, project)

	// 1. Get main build details
	buildURL := fmt.Sprintf("%s/%s/%s/_apis/build/builds/%d?api-version=7.0", s.BaseURL, organisation, project, buildID)
	var build Build
	_, err := s.doRequest("GET", buildURL, patToken, &build)
	if err != nil {
		return nil, fmt.Errorf("failed to get build details: %w", err)
	}

	details := &AgentDetails{
		BuildID:        buildID,
		BuildNumber:    build.BuildNumber,
		DefinitionID:   build.Definition.ID,
		DefinitionName: build.Definition.Name,
		AgentPoolID:    build.Queue.Pool.ID,
		AgentPoolName:  build.Queue.Pool.Name,
		StartTime:      build.StartTime,
		FinishTime:     build.FinishTime,
		BuildStatus:    build.Status,
		BuildResult:    build.Result,
	}

	// 2. Get timeline to find worker name
	timelineURL := fmt.Sprintf("%s/%s/%s/_apis/build/builds/%d/timeline?api-version=7.0", s.BaseURL, organisation, project, buildID)
	var timeline TimelineResponse
	_, err = s.doRequest("GET", timelineURL, patToken, &timeline)
	if err == nil {
		for _, record := range timeline.Records {
			if record.Type == "Job" && record.WorkerName != "" {
				details.AgentName = sql.NullString{String: record.WorkerName, Valid: true}
				break
			}
		}
	} else {
		log.Printf("Could not fetch timeline for build %d: %v", buildID, err)
	}

	// 3. Get logs to find computer name
	logsURL := fmt.Sprintf("%s/%s/%s/_apis/build/builds/%d/logs?api-version=7.0", s.BaseURL, organisation, project, buildID)
	var logs LogsResponse
	_, err = s.doRequest("GET", logsURL, patToken, &logs)
	if err == nil && len(logs.Value) > 0 {
		logURL := logs.Value[0].URL
		req, _ := http.NewRequest("GET", logURL, nil)
		auth := base64.StdEncoding.EncodeToString([]byte(":" + patToken))
		req.Header.Add("Authorization", "Basic "+auth)
		resp, logErr := s.HTTPClient.Do(req)
		if logErr == nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			content := string(body)

			reAgent := regexp.MustCompile(`Agent name: '([^']+)'`)
			reMachine := regexp.MustCompile(`Agent machine name: '([^']+)'`)

			if !details.AgentName.Valid {
				if match := reAgent.FindStringSubmatch(content); len(match) > 1 {
					details.AgentName = sql.NullString{String: match[1], Valid: true}
				}
			}
			if match := reMachine.FindStringSubmatch(content); len(match) > 1 {
				details.ComputerName = sql.NullString{String: match[1], Valid: true}
			}
		}
	} else {
		log.Printf("Could not fetch logs for build %d: %v", buildID, err)
	}

	return details, nil
}

// SyncAgentPools syncs all agent pools for the organization.
func (s *AzureDevOpsAgentsSync) SyncAgentPools(organisation, patToken string) (int, error) {
	log.Printf("🔄 Syncing agent pools for %s", organisation)
	url := fmt.Sprintf("%s/%s/_apis/distributedtask/pools?api-version=7.0", s.BaseURL, organisation)
	var response AgentPoolsResponse
	_, err := s.doRequest("GET", url, patToken, &response)
	if err != nil {
		return 0, fmt.Errorf("error syncing agent pools: %w", err)
	}

	pools := response.Value
	tx, err := s.DB.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
        INSERT INTO azure_devops_agent_pools (organization, pool_id, pool_name, pool_type, size, is_hosted, created_on)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (organization, pool_id) DO UPDATE SET
            pool_name = EXCLUDED.pool_name,
            pool_type = EXCLUDED.pool_type,
            size = EXCLUDED.size,
            is_hosted = EXCLUDED.is_hosted,
            synced_at = CURRENT_TIMESTAMP
    `)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for _, pool := range pools {
		_, err := stmt.Exec(organisation, pool.ID, pool.Name, pool.PoolType, pool.Size, pool.IsHosted, pool.CreatedOn)
		if err != nil {
			return 0, fmt.Errorf("failed to insert/update pool %d: %w", pool.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	log.Printf("✅ Synced %d agent pools for %s", len(pools), organisation)
	return len(pools), nil
}

// SyncAgents syncs all agents for all pools in the organization.
func (s *AzureDevOpsAgentsSync) SyncAgents(organisation, patToken string) (int, error) {
	log.Printf("🔄 Syncing agents for %s", organisation)
	rows, err := s.DB.Query("SELECT pool_id FROM azure_devops_agent_pools WHERE organization = $1", organisation)
	if err != nil {
		return 0, fmt.Errorf("failed to get pools from DB: %w", err)
	}
	defer rows.Close()

	var poolIDs []int
	for rows.Next() {
		var poolID int
		if err := rows.Scan(&poolID); err != nil {
			return 0, err
		}
		poolIDs = append(poolIDs, poolID)
	}

	totalAgents := 0
	for _, poolID := range poolIDs {
		url := fmt.Sprintf("%s/%s/_apis/distributedtask/pools/%d/agents?includeCapabilities=true&api-version=7.0", s.BaseURL, organisation, poolID)
		var response AgentsResponse
		_, err := s.doRequest("GET", url, patToken, &response)
		if err != nil {
			log.Printf("⚠️ Could not sync agents for pool %d: %v", poolID, err)
			continue
		}

		agents := response.Value
		tx, err := s.DB.Begin()
		if err != nil {
			return 0, err
		}

		stmt, err := tx.Prepare(`
            INSERT INTO azure_devops_agents (organization, pool_id, agent_id, agent_name, computer_name, version, status, enabled, os_description, created_on, last_completed_request)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (organization, pool_id, agent_id) DO UPDATE SET
                agent_name = EXCLUDED.agent_name,
                computer_name = EXCLUDED.computer_name,
                version = EXCLUDED.version,
                status = EXCLUDED.status,
                enabled = EXCLUDED.enabled,
                os_description = EXCLUDED.os_description,
                last_completed_request = EXCLUDED.last_completed_request,
                synced_at = CURRENT_TIMESTAMP
        `)
		if err != nil {
			tx.Rollback()
			return 0, err
		}

		for _, agent := range agents {
			computerName := agent.SystemCapabilities["Agent.ComputerName"]
			if computerName == "" {
				computerName = agent.SystemCapabilities["System.HostName"]
			}
			if computerName == "" {
				computerName = agent.Name
			}
			osDescription := agent.SystemCapabilities["Agent.OSDescription"]

			_, err := stmt.Exec(organisation, poolID, agent.ID, agent.Name, computerName, agent.Version, agent.Status, agent.Enabled, osDescription, agent.CreatedOn, agent.LastCompletedRequest)
			if err != nil {
				log.Printf("Failed to insert agent %d for pool %d: %v", agent.ID, poolID, err)
			}
		}
		stmt.Close()
		if err := tx.Commit(); err != nil {
			log.Printf("Failed to commit transaction for pool %d: %v", poolID, err)
		} else {
			log.Printf("✅ Synced %d agents for pool %d", len(agents), poolID)
			totalAgents += len(agents)
		}
	}

	log.Printf("✅ Total agents synced: %d", totalAgents)
	return totalAgents, nil
}

// SyncBuildAgentDetails syncs build agent details for a project.
func (s *AzureDevOpsAgentsSync) SyncBuildAgentDetails(organisation, project, patToken string, maxBuilds int) (int, error) {
	limitText := "(all builds)"
	if maxBuilds > 0 {
		limitText = fmt.Sprintf("(max %d builds)", maxBuilds)
	}
	log.Printf("🔄 Syncing build agent details for %s/%s %s", organisation, project, limitText)

	var allBuilds []Build
	continuationToken := ""
	pageSize := 1000

	for {
		url := fmt.Sprintf("%s/%s/%s/_apis/build/builds?$top=%d&api-version=7.0", s.BaseURL, organisation, project, pageSize)
		if continuationToken != "" {
			url += "&continuationToken=" + continuationToken
		}

		var response BuildsResponse
		headers, err := s.doRequest("GET", url, patToken, &response)
		if err != nil {
			return 0, fmt.Errorf("failed to fetch builds page: %w", err)
		}

		builds := response.Value
		allBuilds = append(allBuilds, builds...)
		log.Printf("   📄 Fetched page: Found %d builds (Total so far: %d)", len(builds), len(allBuilds))

		if maxBuilds > 0 && len(allBuilds) >= maxBuilds {
			allBuilds = allBuilds[:maxBuilds]
			log.Printf("   🎯 Reached max builds limit of %d", maxBuilds)
			break
		}

		continuationToken = headers.Get("X-MS-ContinuationToken")
		if continuationToken == "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("📊 Total builds to process: %d", len(allBuilds))
	syncedBuilds := 0

	tx, err := s.DB.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
        INSERT INTO azure_devops_build_agents (
            organization, project, build_id, build_number, definition_id, 
            definition_name, agent_pool_id, agent_pool_name, agent_id, 
            agent_name, computer_name, start_time, finish_time, build_status, build_result
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (organization, project, build_id) DO UPDATE SET
            build_number = EXCLUDED.build_number,
            definition_id = EXCLUDED.definition_id,
            definition_name = EXCLUDED.definition_name,
            agent_pool_id = EXCLUDED.agent_pool_id,
            agent_pool_name = EXCLUDED.agent_pool_name,
            agent_id = EXCLUDED.agent_id,
            agent_name = EXCLUDED.agent_name,
            computer_name = EXCLUDED.computer_name,
            start_time = EXCLUDED.start_time,
            finish_time = EXCLUDED.finish_time,
            build_status = EXCLUDED.build_status,
            build_result = EXCLUDED.build_result,
            synced_at = CURRENT_TIMESTAMP
    `)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for _, build := range allBuilds {
		details, err := s.GetBuildAgentDetails(organisation, project, patToken, build.ID)
		if err != nil {
			log.Printf("⚠️ Could not sync build %d: %v", build.ID, err)
			continue
		}
		_, err = stmt.Exec(
			organisation, project, details.BuildID, details.BuildNumber, details.DefinitionID,
			details.DefinitionName, details.AgentPoolID, details.AgentPoolName, nil, // agent_id is hard to get reliably
			details.AgentName, details.ComputerName, details.StartTime, details.FinishTime,
			details.BuildStatus, details.BuildResult,
		)
		if err != nil {
			log.Printf("⚠️ Failed to insert build %d: %v", build.ID, err)
			continue
		}
		syncedBuilds++
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	log.Printf("✅ Synced agent details for %d builds", syncedBuilds)
	return syncedBuilds, nil
}

// FullSyncAllProjects performs a complete sync for the organization.
func (s *AzureDevOpsAgentsSync) FullSyncAllProjects(organisation, patToken string, maxBuildsPerProject int) error {
	limitText := "with all builds"
	if maxBuildsPerProject > 0 {
		limitText = fmt.Sprintf("with max %d builds per project", maxBuildsPerProject)
	}
	log.Printf("🚀 Starting full sync for organization: %s %s", organisation, limitText)

	if err := s.InitializeSchema(); err != nil {
		return err
	}

	projects, err := s.GetAllProjects(organisation, patToken)
	if err != nil {
		return err
	}

	poolCount, err := s.SyncAgentPools(organisation, patToken)
	if err != nil {
		return err
	}

	agentCount, err := s.SyncAgents(organisation, patToken)
	if err != nil {
		return err
	}

	var totalBuildCount int
	var wg sync.WaitGroup
	// Limit concurrency to avoid overwhelming the API or database
	sem := make(chan struct{}, 4)

	for _, project := range projects {
		wg.Add(1)
		go func(p Project) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			log.Printf("\n🔄 Processing project: %s", p.Name)
			buildCount, err := s.SyncBuildAgentDetails(organisation, p.Name, patToken, maxBuildsPerProject)
			if err != nil {
				log.Printf("⚠️ Failed to sync project %s: %v", p.Name, err)
			} else {
				totalBuildCount += buildCount
			}
		}(project)
	}
	wg.Wait()

	log.Println("\n🎉 Full sync completed:")
	log.Printf("   - Projects Found: %d", len(projects))
	log.Printf("   - Agent Pools Synced: %d", poolCount)
	log.Printf("   - Agents Synced: %d", agentCount)
	log.Printf("   - Total Build Agent Details Synced: %d", totalBuildCount)

	return nil
}

// Close closes the database connection.
func (s *AzureDevOpsAgentsSync) Close() {
	if s.DB != nil {
		s.DB.Close()
	}
}

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, relying on environment variables")
	}

	organisation := os.Getenv("ORGANIZATION")
	patToken := os.Getenv("AZURE_DEVOPS_TOKEN")

	if organisation == "" || patToken == "" {
		log.Fatal("❌ Missing required environment variables: ORGANIZATION, AZURE_DEVOPS_TOKEN")
	}

	maxBuildsPerProject := 0
	if len(os.Args) > 1 {
		val, err := strconv.Atoi(os.Args[1])
		if err != nil {
			log.Fatalf("❌ Invalid number for max builds per project: %s", os.Args[1])
		}
		maxBuildsPerProject = val
	}

	syncClient, err := NewAzureDevOpsAgentsSync()
	if err != nil {
		log.Fatalf("Failed to create sync client: %v", err)
	}
	defer syncClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- syncClient.FullSyncAllProjects(organisation, patToken, maxBuildsPerProject)
	}()

	select {
	case err := <-done:
		if err != nil {
			log.Fatalf("Sync failed: %v", err)
		}
		log.Println("Sync completed successfully.")
	case <-ctx.Done():
		log.Fatal("Sync timed out after 30 minutes.")
	}
}
