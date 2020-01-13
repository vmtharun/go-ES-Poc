package main

import "testing"

func TestGenerateCollection(t *testing.T) {
	var (
		articles []Article
	)

	// created dummy data
	generateCollection(&articles)
	if len(articles) != 10 {
		t.Error("Expected 10, got ", len(articles))
	}
}
