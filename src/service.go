package main

import (
	"bytes"
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

func postSparqlUpdate(update *string) {
	log.Println("start post update")
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

func main() {
	log.Println("Starting SPARQL Sink")
	loadConfig()
	var nodeMetadataFetchError = fetchNodeMetadata()
	if nodeMetadataFetchError != nil {
		log.Panic(nodeMetadataFetchError)
	}

	r := gin.Default()

	r.POST("/:graph/entities", func(c *gin.Context) {
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
		insertSparql.WriteString("\nINSERT { \n")
		whereSparql.WriteString("\nWHERE { \n VALUES ?subject { \n")

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
					// ^^<http://www.w3.org/2001/XMLSchema#integer>
					insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + strconv.Itoa(v) + "\"^^<http://www.w3.org/2001/XMLSchema#integer> . \n")
				case string:
					// check if its datatyped
					if strings.HasPrefix(v, "~:") {
						objURI := expandCURI(v[2:])
						insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> <" + objURI + "> . \n")
					} else {
						insertSparql.WriteString(" <" + eidURI + "> <" + predicate + "> \"" + v + "\" . \n")
					}
				default:
					// log.Printf("Unknown type %T!\n", v)
				}
			}
		}

		// close sparql sections
		insertSparql.WriteString("} \n")
		whereSparql.WriteString("} \n }")

		// execute POST to sparql endpoint
		body := deleteSparql.String() + insertSparql.String() + whereSparql.String()
		postSparqlUpdate(&body)

		c.Status(http.StatusOK)
	})

	r.Run(":" + port)
}
