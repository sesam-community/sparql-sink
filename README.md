# sparql-sink
Generic SPARQL Sink

# Config

Set the following environment variables:

SERVICE_PORT default to 5000 if not set.

SESAM_JWT - a JWT token to talk to the node

SESAM_API - the SESAM_API endpoint 

SPARQL_ENDPOINT - the SPARQL endpoint to write to.

# JSON Pipe Config

Setup a pipe in Sesam to push to this microservice system. The sink should be the JSON Push sink. 

remove_namespaces : false




