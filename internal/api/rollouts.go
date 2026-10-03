package api

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/Bob-xisuke/orb-rollout-probe/internal/store"
)

// createRolloutInput is the accepted JSON body of POST /v1/rollouts. Pointers
// distinguish an omitted numeric/boolean field from its zero value so required
// fields can be enforced.
type createRolloutInput struct {
	Namespace           string             `json:"namespace"`
	Workload            string             `json:"workload"`
	TargetVersion       string             `json:"targetVersion"`
	BatchSize           *int               `json:"batchSize"`
	MaxUnavailableRatio *float64           `json:"maxUnavailableRatio"`
	CompletedBatches    *int               `json:"completedBatches"`
	Probe               *probeInput        `json:"probe"`
	ProbeResults        []probeResultInput `json:"probeResults"`
	Rollback            *bool              `json:"rollback"`
}

type probeInput struct {
	Type             *string `json:"type"`
	Path             *string `json:"path"`
	Port             *int    `json:"port"`
	TimeoutSeconds   *int    `json:"timeoutSeconds"`
	SuccessThreshold *int    `json:"successThreshold"`
	FailureThreshold *int    `json:"failureThreshold"`
}

type probeResultInput struct {
	Batch      int     `json:"batch"`
	Ready      *bool   `json:"ready"`
	FailedPods *int    `json:"failedPods"`
	Reason     *string `json:"reason,omitempty"`
}

func registerRolloutRoutes(router *gin.Engine, st *store.Store) {
	router.POST("/v1/rollouts", createRolloutHandler(st))
	router.GET("/v1/rollouts", getRolloutHandler(st))
}

func createRolloutHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var input createRolloutInput
		decoder := json.NewDecoder(c.Request.Body)
		if err := decoder.Decode(&input); err != nil {
			invalidRollout(c, "request body must be a single well-formed rollout object")
			return
		}
		if err := decoder.Decode(new(struct{})); err != io.EOF {
			invalidRollout(c, "request body must be a single rollout object")
			return
		}

		rollout, err := rolloutFromInput(&input)
		if err != nil {
			invalidRollout(c, err.Error())
			return
		}

		created, err := st.CreateRollout(c.Request.Context(), rollout)
		if err != nil {
			if errors.Is(err, store.ErrRolloutConflict) {
				c.JSON(http.StatusConflict, gin.H{"error": gin.H{
					"code":    "RolloutConflictError",
					"message": "a rollout with the same namespace, workload and targetVersion already exists with different content",
				}})
				return
			}
			respondStorageError(c, err)
			return
		}

		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		c.JSON(status, rollout)
	}
}

func getRolloutHandler(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		namespace := strings.TrimSpace(c.Query("namespace"))
		workload := strings.TrimSpace(c.Query("workload"))
		targetVersion := strings.TrimSpace(c.Query("targetVersion"))
		if namespace == "" || workload == "" || targetVersion == "" {
			invalidRollout(c, "namespace, workload and targetVersion query parameters are all required")
			return
		}

		rollout, err := st.GetRollout(c.Request.Context(), namespace, workload, targetVersion)
		if err != nil {
			if errors.Is(err, store.ErrRolloutNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"error": gin.H{
					"code":    "RolloutNotFoundError",
					"message": "no rollout matches the given namespace, workload and targetVersion",
				}})
				return
			}
			respondStorageError(c, err)
			return
		}
		c.JSON(http.StatusOK, rollout)
	}
}

// rolloutFromInput validates input and converts it into the stored record.
func rolloutFromInput(input *createRolloutInput) (*store.Rollout, error) {
	if strings.TrimSpace(input.Namespace) == "" {
		return nil, errors.New("namespace must be a non-empty string")
	}
	if strings.TrimSpace(input.Workload) == "" {
		return nil, errors.New("workload must be a non-empty string")
	}
	if strings.TrimSpace(input.TargetVersion) == "" {
		return nil, errors.New("targetVersion must be a non-empty string")
	}
	if input.BatchSize == nil {
		return nil, errors.New("batchSize is required")
	}
	if *input.BatchSize <= 0 {
		return nil, errors.New("batchSize must be a positive integer")
	}
	if input.MaxUnavailableRatio == nil {
		return nil, errors.New("maxUnavailableRatio is required")
	}
	ratio := *input.MaxUnavailableRatio
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
		return nil, errors.New("maxUnavailableRatio must be between 0 and 1 inclusive")
	}
	completed := 0
	if input.CompletedBatches != nil {
		completed = *input.CompletedBatches
	}
	if completed < 0 || completed > *input.BatchSize {
		return nil, errors.New("completedBatches must be between 0 and batchSize inclusive")
	}
	if input.Rollback == nil {
		return nil, errors.New("rollback is required")
	}

	probe, err := probeFromInput(input.Probe)
	if err != nil {
		return nil, err
	}

	results := make([]store.ProbeResult, 0, len(input.ProbeResults))
	for i := range input.ProbeResults {
		result, err := probeResultFromInput(&input.ProbeResults[i])
		if err != nil {
			return nil, err
		}
		results = append(results, *result)
	}

	return &store.Rollout{
		Namespace:           input.Namespace,
		Workload:            input.Workload,
		TargetVersion:       input.TargetVersion,
		BatchSize:           *input.BatchSize,
		MaxUnavailableRatio: ratio,
		CompletedBatches:    completed,
		Probe:               *probe,
		ProbeResults:        results,
		Rollback:            *input.Rollback,
	}, nil
}

func probeFromInput(input *probeInput) (*store.Probe, error) {
	if input == nil {
		return nil, errors.New("probe is required and must be complete")
	}
	if input.Type == nil || strings.TrimSpace(*input.Type) == "" {
		return nil, errors.New("probe.type must be a non-empty string")
	}
	if input.Path == nil || strings.TrimSpace(*input.Path) == "" {
		return nil, errors.New("probe.path must be a non-empty string")
	}
	if input.Port == nil {
		return nil, errors.New("probe.port is required")
	}
	if *input.Port < 1 || *input.Port > 65535 {
		return nil, errors.New("probe.port must be between 1 and 65535")
	}
	if input.TimeoutSeconds == nil || *input.TimeoutSeconds <= 0 {
		return nil, errors.New("probe.timeoutSeconds must be a positive integer")
	}
	if input.SuccessThreshold == nil || *input.SuccessThreshold <= 0 {
		return nil, errors.New("probe.successThreshold must be a positive integer")
	}
	if input.FailureThreshold == nil || *input.FailureThreshold <= 0 {
		return nil, errors.New("probe.failureThreshold must be a positive integer")
	}
	return &store.Probe{
		Type:             *input.Type,
		Path:             *input.Path,
		Port:             *input.Port,
		TimeoutSeconds:   *input.TimeoutSeconds,
		SuccessThreshold: *input.SuccessThreshold,
		FailureThreshold: *input.FailureThreshold,
	}, nil
}

func probeResultFromInput(input *probeResultInput) (*store.ProbeResult, error) {
	if input.Batch < 0 {
		return nil, errors.New("probeResults.batch must not be negative")
	}
	if input.Ready == nil {
		return nil, errors.New("probeResults.ready is required")
	}
	failedPods := 0
	if input.FailedPods != nil {
		failedPods = *input.FailedPods
	}
	if failedPods < 0 {
		return nil, errors.New("probeResults.failedPods must not be negative")
	}
	result := &store.ProbeResult{
		Batch:      input.Batch,
		Ready:      *input.Ready,
		FailedPods: failedPods,
	}
	if input.Reason != nil {
		result.Reason = *input.Reason
	}
	return result, nil
}

func invalidRollout(c *gin.Context, message string) {
	c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{
		"code":    "InvalidRolloutInputError",
		"message": message,
	}})
}

func respondStorageError(c *gin.Context, err error) {
	log.Printf("rollout storage error: %v", err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": gin.H{
		"code":    "internal_error",
		"message": "the request could not be completed due to a storage problem",
	}})
}
