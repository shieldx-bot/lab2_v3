package redismetric

import (
	"testing"
)

func TestName(t *testing.T) {
	plugin := &RedisMetricPlugin{}
	if name := plugin.Name(); name != Name {
		t.Errorf("expected %s, got %s", Name, name)
	}
}

func TestScoreExtensions(t *testing.T) {
	plugin := &RedisMetricPlugin{}
	if ext := plugin.ScoreExtensions(); ext != nil {
		t.Errorf("expected nil, got %v", ext)
	}
}

func TestIsNodeInTopNodes(t *testing.T) {
	plugin := &RedisMetricPlugin{}

	tests := []struct {
		name     string
		nodeName string
		topNodes []string
		expected bool
	}{
		{
			name:     "Node is in top nodes",
			nodeName: "node-1",
			topNodes: []string{"k3d-node-agent-0"},
			expected: true,
		},
		{
			name:     "Node is not in top nodes",
			nodeName: "node-3",
			topNodes: []string{"k3d-node-agent-1"},
			expected: false,
		},
		{
			name:     "Empty top nodes",
			nodeName: "node-1",
			topNodes: []string{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := plugin.isNodeInTopNodes(tt.nodeName, tt.topNodes); got != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestCalculateScore(t *testing.T) {
	plugin := &RedisMetricPlugin{}

	if score := plugin.calculateScore("node-1", []string{"node-1", "node-2"}); score != 100 {
		t.Errorf("expected 100, got %d", score)
	}

	if score := plugin.calculateScore("node-3", []string{"node-1", "node-2"}); score != 0 {
		t.Errorf("expected 0, got %d", score)
	}
}
