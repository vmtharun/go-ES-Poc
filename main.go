package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/v7"
)

// Tag element
type Tag struct {
	Name string `json:"name"`
}

// Item object
type Item struct {
	Interactions []string `json:"interactions"`
	Tags         []Tag    `json:"tags"`
}

// Article object
type Article struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	Published time.Time `json:"published"`
	Items     Item      `json:"items"`
	Tags      []Tag     `json:"tags"`
}

const indexName = "testindex"

func main() {
	log.SetFlags(0)

	var (
		articles []Article
	)

	// created dummy data
	generateCollection(&articles)

	// configure ES client
	es := configES()

	log.Println(strings.Repeat("~", 37))

	createIndice(es)

	log.Println(strings.Repeat("-", 37))
	indexArticle(articles, es)

	log.Println(strings.Repeat("=", 37))
}

// Dummy data to load to es Index
func generateCollection(articles *[]Article) {
	const count = 10
	// Generate the articles collection
	//
	for i := 1; i < count+1; i++ {
		countStr := strconv.Itoa(i)
		*articles = append(*articles, Article{
			ID:        i,
			Title:     strings.Join([]string{"Title", strconv.Itoa(i)}, " "),
			Body:      "Lorem ipsum dolor sit amet...",
			Published: time.Now().Round(time.Second).UTC().AddDate(0, 0, i),
			Tags: []Tag{
				Tag{Name: "TagZ-" + countStr},
				Tag{Name: "TagY-" + countStr},
				Tag{Name: "TagW-" + countStr},
			},
			Items: Item{
				Interactions: []string{"Interaction A" + countStr, "Interaction B" + countStr},
				Tags: []Tag{
					Tag{Name: "TagA-" + countStr},
					Tag{Name: "TagB-" + countStr},
					Tag{Name: "TagC-" + countStr},
				},
			},
		})
	}
	log.Printf("> Generated %d articles", len(*articles))
}

// Configure Es client
func configES() *elasticsearch.Client {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	return es
}

/* Define mapping for the Index */
func getMapping() map[string]interface{} {
	mapping := map[string]interface{}{
		"settings": map[string]interface{}{
			"number_of_shards": 1,
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"id": map[string]interface{}{"type": "integer"},
				"items": map[string]interface{}{
					"properties": map[string]interface{}{
						"interactions": map[string]interface{}{
							"enabled": false,
							"type":    "nested",
						},
						"tags": map[string]interface{}{
							"type":            "nested",
							"include_in_root": true,
							"properties": map[string]interface{}{
								"name": map[string]interface{}{
									"type": "keyword",
								},
							},
						},
					},
				},
			},
		},
	}

	return mapping
}

/* Checks for existance of Index and create new index
if not exists.
Here we need to provide the Mapping, Analyzer or any other setting to Index */
func createIndice(es *elasticsearch.Client) {
	res, err := es.Indices.Exists([]string{indexName})
	if res.StatusCode == 404 {
		var buf bytes.Buffer
		// get Mapping for int index
		mappings := getMapping()

		if err := json.NewEncoder(&buf).Encode(mappings); err != nil {
			log.Fatalf("Error encoding query: %s", err)
		}
		res, err = es.Indices.Create(indexName,
			es.Indices.Create.WithContext(context.Background()),
			es.Indices.Create.WithBody(&buf),
		)
		if err != nil {
			log.Fatalf("Cannot create index: %s", err)
		}
		if res.IsError() {
			log.Fatalf("Cannot create index: %s", res)
		}
	}
}

/* Save data to already created Index.
Pass the data and es client */
func indexArticle(articles []Article, es *elasticsearch.Client) {
	var wg sync.WaitGroup
	for _, article := range articles {
		wg.Add(1)

		go func(article Article) {
			defer wg.Done()

			var buf bytes.Buffer
			if err := json.NewEncoder(&buf).Encode(article); err != nil {
				log.Fatalf("Error encoding query: %s", err)
			}
			// Set up the request object.
			req := esapi.IndexRequest{
				Index:      "testindex",
				DocumentID: strconv.Itoa(article.ID),
				Body:       &buf,
				Refresh:    "true",
			}

			// Perform the request with the client.
			res, err := req.Do(context.Background(), es)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			defer res.Body.Close()

			if res.IsError() {
				log.Printf("[%s] Error indexing document ID=%d", res.Status(), article.ID)
			} else {
				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
				} else {
					// Print the response status and indexed document version.
					log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
				}
			}
		}(article)
	}
	wg.Wait()
}
