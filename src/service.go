package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	gabs "github.com/Jeffail/gabs/v2"
	gin "github.com/gin-gonic/gin"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	debugLevel        string
	sesamAPI          string
	sesamJWT          string
	sparqlEndpoint    string
	graphBase         string
	port              string
	namespaceMappings map[string]string
)

func loadConfig() {
	// get config
	log.Println("Loading Config ---------------------- ")

	sesamAPI = os.Getenv("SESAM_API")
	sesamJWT = os.Getenv("SESAM_JWT")
	graphBase = os.Getenv("GRAPH_BASE")

	port = os.Getenv("SERVICE_PORT")
	if port == "" {
		port = "5000"
	}

	debugLevel = os.Getenv("DEBUG_LEVEL")

	sparqlEndpoint = os.Getenv("SPARQL_ENDPOINT")

	log.Println("PORT: " + port)
	log.Println("SPARQL: " + sparqlEndpoint)
	log.Println("Loaded Config  ---------------------- ")
}

func getJSON(path string) ([]byte, error) {
	var url = sesamAPI + path
	var client = &http.Client{
		Timeout: time.Second * 10,
	}

	var req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+sesamJWT)

	var resp, err = client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return bodyBytes, nil
}

// checks if the dataset has been modified today
func isDatasetModified(dataset string) (bool, error) {
	// get json for dataset
	jsonData, _ := getJSON("/datasets/" + dataset)
	jsonParsed, _ := gabs.ParseJSON(jsonData)
	modifiedOn := jsonParsed.Path("runtime.last-modified").String()[1:11]

	current := time.Now()
	ctimeStr := current.Format("2006-01-02")

	changed := (ctimeStr == modifiedOn)
	return changed, nil
}

func postSparqlUpdate(update *string) {
	log.Println("start post update")
	// log.Println(*update)
	var url = sparqlEndpoint
	var client = &http.Client{
		Timeout: time.Second * 10,
	}
	var req, _ = http.NewRequest("POST", url, bytes.NewBufferString(*update))
	req.Header.Set("Content-Type", "application/sparql-update")

	var resp, err = client.Do(req)
	if err != nil {
		log.Println("Error in sparql update request: " + err.Error())
		return
	}

	if resp.StatusCode != 200 {
		log.Println("Error " + resp.Status)
	} else {
		log.Println("Success " + resp.Status)
	}

	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Error reading response")
		return
	}
}

func fetchNodeMetadata() error {
	jsonData, err := getJSON("/metadata")
	if err != nil {
		return err
	}
	namespaceMappings = make(map[string]string)
	jsonParsed, _ := gabs.ParseJSON(jsonData)
	nsMappings := jsonParsed.Path("config.effective.namespaces.default")
	for key, child := range nsMappings.ChildrenMap() {
		namespaceMappings[key] = child.Data().(string)
	}
	return nil
}

func contains(list []string, item string) bool {
	for _, a := range list {
		if a == item {
			return true
		}
	}
	return false
}

func expandCURI(curi string) string {
	nsstrings := strings.Split(curi, ":")
	if len(nsstrings) == 2 {
		prefix := nsstrings[0]
		suffix := nsstrings[1]
		expansion := namespaceMappings[prefix]
		uri := expansion + suffix
		return uri
	}

	log.Println("No namespace detected using default. Original value: " + curi)
	return "http://example.org/1"
}

func convertSingleObject(obj map[string]interface{}) string {
	for _, i := range obj {
		switch v := i.(type) {
		case int:
			log.Println("int")
		case string:
			log.Println("string " + v)
			if strings.HasPrefix(v, "~t") {
				ts := v[2:]
				log.Println(ts)
			}
		case float64:
			// var s string = strconv.FormatFloat(v, )
			// s := fmt.Sprintf("%f", int(v.(float64)))
			if v == float64(int64(v)) {
				i := int(v)
				s := strconv.Itoa(i)
				log.Println(s)
			} else {
				s := fmt.Sprintf("%f", v)
				log.Println(s)
			}
		case []interface{}:
			for _, ai := range v {
				switch v := ai.(type) {
				case int:
					log.Println("int")
				case string:
					log.Println("string " + v)
				default:
					log.Println("unknown list type")
				}
			}
		default:
			log.Printf("Unknown type %T!\n", v)
		}
	}
	return ""
}

func buildSparqlAdd(graph string, entities []*map[string]interface{}) string {
	var insertSparql strings.Builder

	insertSparql.WriteString("\nINSERT DATA { GRAPH <" + graphBase + graph + "> { \n")

	numentities := fmt.Sprintf("number of entities to convert to sparql : %v", len(entities))
	log.Println(numentities)
	// Iterate objects in array
	for _, entity := range entities {
		// expand _id with namespace expansion
		eidURI := expandCURI((*entity)["_id"].(string))

		if val, ok := (*entity)["_deleted"]; ok {
			if val.(bool) == true {
				continue
			}
		}

		for k, i := range *entity {
			if strings.HasPrefix(k, "_") {
				continue
			}

			if strings.HasPrefix(k, "$ids") {
				// process list and add sameas refs
				// todo.
				continue
			}

			// log.Printf("key[%s] value[%s]\n", k, i)
			// expand predicate
			predicate := expandCURI(k)
			if predicate == "" {
				log.Println("No namespace on key : " + k)
				continue
			}

			switch v := i.(type) {
			case float64:
				if v == float64(int64(v)) {
					insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + strconv.Itoa(int(v)) + "\"^^<http://www.w3.org/2001/XMLSchema#integer> . \n")
				} else {
					s := fmt.Sprintf("%f", v)
					insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + s + "\"^^<http://www.w3.org/2001/XMLSchema#float> . \n")
				}
			case int:
				insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + strconv.Itoa(v) + "\"^^<http://www.w3.org/2001/XMLSchema#integer> . \n")
			case string:
				// check if its datatyped
				if strings.HasPrefix(v, "~:") {
					objURI := expandCURI(v[2:])
					insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> <" + objURI + "> . \n")
				} else if strings.HasPrefix(v, "~t") {
					insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + v[2:] + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . \n")
				} else {
					insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + v + "\" . \n")
				}
			case []interface{}:
				for _, ai := range v {
					switch v := ai.(type) {
					case string:
						if strings.HasPrefix(v, "~:") {
							objURI := expandCURI(v[2:])
							insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> <" + objURI + "> . \n")
						} else if strings.HasPrefix(v, "~t") {
							insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + v[2:] + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . \n")
						} else {
							insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + v + "\" . \n")
						}
					case float64:
						if v == float64(int64(v)) {
							insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + strconv.Itoa(int(v)) + "\"^^<http://www.w3.org/2001/XMLSchema#integer> . \n")
						} else {
							s := fmt.Sprintf("%f", v)
							insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + s + "\"^^<http://www.w3.org/2001/XMLSchema#float> . \n")
						}
					default:
						log.Println("unknown list type")
					}
				}
			default:
				// log.Printf("Unknown type %T!\n", v)
			}
		}
	}

	// close sparql sections
	insertSparql.WriteString(" } }\n")

	body := insertSparql.String()
	return body
}

func main() {
	log.Println("Starting SPARQL Sink")
	loadConfig()
	var nodeMetadataFetchError = fetchNodeMetadata()
	if nodeMetadataFetchError != nil {
		log.Panic(nodeMetadataFetchError)
	}

	r := gin.Default()

	r.POST("snapshot/:graph/:dataset", func(c *gin.Context) {
		log.Println("Publish Snapshot")

		graph := c.Param("graph")
		dataset := c.Param("dataset")
		current := time.Now()
		gtime := current.Format("2006-01-02")
		graph = graph + "-" + gtime

		modified, _ := isDatasetModified(dataset)

		if !modified {
			log.Println("Dataset has not changed so not snapshop is written. Dataset is " + dataset)
			c.Status(http.StatusOK)
			return
		}

		var url = sesamAPI + "datasets/" + dataset + "/entities"
		var client = &http.Client{
			Timeout: time.Second * 360,
		}

		var req, _ = http.NewRequest("GET", url, nil)
		req.Header.Set("Authorization", "Bearer "+sesamJWT)

		var resp, _ = client.Do(req)
		defer resp.Body.Close()

		dec := json.NewDecoder(resp.Body)

		// read open bracket
		_, err := dec.Token()
		if err != nil {
			log.Fatal(err)
		}

		var batchSize = 100
		var entities []*map[string]interface{}
		var count = 0

		var m *map[string]interface{}

		// while the array contains values
		for dec.More() {
			m = new(map[string]interface{})
			err := dec.Decode(m)
			if err != nil {
				log.Fatal(err)
			}

			entities = append(entities, m)
			count++

			if count == batchSize {
				var sparql = buildSparqlAdd(graph, entities)
				log.Println("sparql update : " + sparql)
				postSparqlUpdate(&sparql)
				entities = make([]*map[string]interface{}, 0)
				count = 0
			}
		}

		// read closing bracket
		_, err = dec.Token()
		if err != nil {
			log.Fatal(err)
		}

		if count > 0 {
			var sparql = buildSparqlAdd(graph, entities)
			log.Println("sparql update : " + sparql)
			postSparqlUpdate(&sparql)
		}

		c.Status(http.StatusOK)
	})

	r.POST("store/:graph/entities", func(c *gin.Context) {
		log.Println("Process Entities")
		// get body
		graph := c.Param("graph")
		data, _ := c.GetRawData()
		jsonParsed, _ := gabs.ParseJSON(data)

		var deleteSparql strings.Builder
		var insertSparql strings.Builder
		var whereSparql strings.Builder

		deleteSparql.WriteString("WITH <" + graphBase + graph + "> \n")
		deleteSparql.WriteString("DELETE { ?subject ?p ?o } \n")
		whereSparql.WriteString("\nWHERE { \n VALUES ?subject { \n")

		insertSparql.WriteString("\nINSERT DATA { GRAPH <" + graphBase + graph + "> { \n")

		// Iterate objects in array
		for _, e := range jsonParsed.Children() {
			entity := e.Data().(map[string]interface{})
			// log.Println(entity)

			// expand _id with namespace expansion
			eidURI := expandCURI(entity["_id"].(string))
			whereSparql.WriteString("<")
			whereSparql.WriteString(eidURI)
			whereSparql.WriteString(">\n")

			if val, ok := entity["_deleted"]; ok {
				if val.(bool) == true {
					continue
				}
			}

			for k, i := range entity {
				if strings.HasPrefix(k, "_") {
					continue
				}

				if strings.HasPrefix(k, "$ids") {
					// process list and add sameas refs
					// todo.
					continue
				}

				// log.Printf("key[%s] value[%s]\n", k, i)
				// expand predicate
				predicate := expandCURI(k)
				if predicate == "" {
					log.Println("No namespace on key : " + k)
					continue
				}

				switch v := i.(type) {
				case int:
					insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + strconv.Itoa(v) + "\"^^<http://www.w3.org/2001/XMLSchema#integer> . \n")
				case float64:
					if v == float64(int64(v)) {
						insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + strconv.Itoa(int(v)) + "\"^^<http://www.w3.org/2001/XMLSchema#integer> . \n")
					} else {
						s := fmt.Sprintf("%f", v)
						insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + s + "\"^^<http://www.w3.org/2001/XMLSchema#float> . \n")
					}
				case string:
					// check if its datatyped
					if strings.HasPrefix(v, "~:") {
						objURI := expandCURI(v[2:])
						insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> <" + objURI + "> . \n")
					} else if strings.HasPrefix(v, "~t") {
						insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + v[2:] + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . \n")
					} else {
						insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + v + "\" . \n")
					}
				case []interface{}:
					for _, ai := range v {
						switch v := ai.(type) {
						case string:
							if strings.HasPrefix(v, "~:") {
								objURI := expandCURI(v[2:])
								insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> <" + objURI + "> . \n")
							} else if strings.HasPrefix(v, "~t") {
								insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + v[2:] + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . \n")
							} else {
								insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + v + "\" . \n")
							}
						case float64:
							if v == float64(int64(v)) {
								insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + strconv.Itoa(int(v)) + "\"^^<http://www.w3.org/2001/XMLSchema#integer> . \n")
							} else {
								s := fmt.Sprintf("%f", v)
								insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + s + "\"^^<http://www.w3.org/2001/XMLSchema#float> . \n")
							}
						default:
							log.Println("unknown list type")
						}
					}
				default:
					// log.Printf("Unknown type %T!\n", v)
				}
			}
		}

		// close sparql sections
		insertSparql.WriteString("} }\n")
		whereSparql.WriteString(" \n } \n ?subject ?p ?o  }; \n ")

		// execute POST to sparql endpoint
		body := deleteSparql.String() + whereSparql.String() + insertSparql.String()
		postSparqlUpdate(&body)

		c.Status(http.StatusOK)
	})

	r.Run(":" + port)
}
