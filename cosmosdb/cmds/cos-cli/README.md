# cos-cli

The `cos-cli` is a simple cmd line command to interact with collections in a Core Cosmos-db database (the one with SQL Api interface... as to speak). It's been written because I didn't find any other
tool that could fill the needs of some use cases I have been working on lately.

Run the command:

```
./cos-cli
```

Expected output:

```
Usage of ./cos-cli:
  -cfg string
        yaml file of command args (default: cos-cli-cfg.yml)
  -cmd string
        cmd: select, upsert, delete (default: select)
  -cnt string
        container name or id (resolved by the lks file) (default: )
  -concurrency-level int
        level of concurrency in modify ops  (default: 1)
  -context-query string
        cosmos context query statement to get values for the actual target query (default: )
  -cos string
        cosmos instance config name (default: default)
  -db string
        db name or id (resolved by the lks file) (default: )
  -delete
        option to delete queried docs  (default: false)
  -limit int
        limit the number of records returned (default: 0)
  -lks-file string
        yaml file of cosmos config (connection string and optionally db and collection resolution) (default: lks-cfg.yml)
  -log-level int
        log level to be used (default: -1)
  -out string
        output-file (default: cos-cli.out)
  -page-size int
        page size used in the paged select ops (default: 500)
  -print string
        cosmos print template for queried records (default: {{ .id }}:{{ .id }}:{{ .json }})
  -query string
        cosmos query statement (default: select * from c)
8:59AM FTL cos-cli::main error="db name not specified"
```

The output shows the usage if no params or wrong params have been provided. The table below list the different params and their default values.

| parameter         | default                          | note                                                                                                                                                                                                                                                          |
|-------------------|----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cfg               |                                  | one of the two config files. this one can  provide all the required params for the execution and is a means to provide params without getting not so easy command lines; cmd line params take precedence over values provided in the config file              |
| cmd               | select                           | the type of command to execute; at the time of writing `select` only (but with the possibility to use the modifier `delete` flag                                                                                                                              |
| cnt               |                                  | the name of the container: used as is or resolved by the values in the `lks-file` config file                                                                                                                                                                 |                                                                                                                                                   |
| concurrency-level | 1                                | the level of concurrency in data modification operation (delete, ...), not used for simple select.                                                                                                                                                            |
| context-query     |                                  | This is a query used to customize the actual query that is made, the idea is to execute this query and use the result to customize the query specified by the `query` params (see example below); used to do sort of *select where ... in*  type of statement |
| cos               | default                          | specified the instance name of the cosmsodb to be connected to and is searched in the `lks-file`                                                                                                                                                              |
| db                |                                  | the name of the db: used as is or resolved by the values in the `lks-file` config file                                                                                                                                                                        |
| delete            | false                            | it's a modified of the `select` command and istructs the to delete the records returned by the query                                                                                                                                                          |
| limit             | 0                                | limit the number of documents returned by a query                                                                                                                                                                                                             |
| lks-file          | lks-cfg.yml                      | config file that contains information about the cosmos-db to connect to and other information to translate reference of db and container names                                                                                                                |
| log-level         | -1                               | log level with values applicable to the the log-zero library                                                                                                                                                                                                  |
| out               | cos-cli.out                      | unused yet                                                                                                                                                                                                                                                    |
| page-size         | 500                              | the size used by select in paging the returned documents                                                                                                                                                                                                      |
| print             | {{ .id }}:{{ .id }}:{{ .json }}) | golang template to print the output of aretrieved document in the select operations                                                                                                                                                                           |
| query             | `select * from c`                | actual query text                                                                                                                                                                                                                                             |
| title             |                                  | this parameter can only be used in the `cfg` file and not from command line                                                                                                                                                                                   |

## Examples

### Cmd line invocation

```
export LEAS_CAB_COSDB_ACCTKEY="..."
export LEAS_CAB_COSDB_DBNAME="..."
export LEAS_CAB_COSDB_ENDPOINT="https://....documents.azure.com:443/"
export LEAS_CAB_CAMPAIGN="BPMGM1"

./cos-cli  -cmd select -db leas_cab_db -cnt "tokens" -query "select count(1) as count from c where c.pkey = 'campaign'" -print "::{{ .json }}"
```

The env variables are required  because referenced by the default configs...

### lks-file invocation

An example of this type of file is provided in: [lks-cfg-sample.yml](lks-cfg-sample.yml)

## examples using the `cfg` file
An example of this type of file is provided in: [sample-001-cfg.yml](sample-001-cfg.yml) and commented below.

The following config file contains the operation to query a number of collections in the DB. 

- At the top there is a reference to general config information: the log level and config file reference for the connection to CosmosDb.
- 2 ops that are plain selects. The table below details the first one.

| cfg         | value          | note                                                                                                                                                   |
|-------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| cmd         | select         | it is a plain select; no delete flag or other modifier has been set                                                                                    | 
| title       | count campaign | it's a string that will be put in the output to identify different output for different operations                                                     |
| query       | select ...     | the actual query, it's a count                                                                                                                         |
| cnt         | campaign       | the container to target (the value is used as is or resolved by the linked service file                                                                | 
| concurrency | 3              | the level of concurrency that would be used if it where a modifier statement; in this case is not used and, if you note, is not reported in the output |
| print       | ::{{ .json }}  | every row of output is printed as a pair of `:` followed by the json representation of the document                                                    |

The secondo one is very similar to the first but with a main difference: the presence of a `context-query` param. The query it's a count but with a parameter that is taken from the output of the
`context-query` text.

| cfg           | value                                                                                         | note                                                                                                                                                   |
|---------------|-----------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| cmd           | select                                                                                        | it is a plain select; no delete flag or other modifier has been set                                                                                    | 
| title         | count rows for file for ${LEAS_CAB_CAMPAIGN}                                                  | it's a string that will be put in the output to identify different output for different operations                                                     |
| query         | `select count(1) as count from c where c.pkey = '${id}'`                                      | the actual query, it's a count                                                                                                                         |
| cnt           | files                                                                                         | the container to target (the value is used as is or resolved by the linked service file                                                                | 
| context-query | `select * from c where c.pkey = 'cos-text-file' and STARTSWITH(c.id, '${LEAS_CAB_CAMPAIGN}')` | this query is executed first; for each document extracted the real query gets evaluated and the executed to get the actual output                      |
| concurrency   | 3                                                                                             | the level of concurrency that would be used if it where a modifier statement; in this case is not used and, if you note, is not reported in the output |
| print         | ::{{ .json }}                                                                                 | every row of output is printed as a pair of `:` followed by the json representation of the document                                                    |


```
db: rtp_bconn_db
lks-file: lks-sample.yml
log-level: 1
ops:
  # count campaign
  - cmd: select
    title: count campaign
    query: "select count(1) as count from c where c.pkey = 'campaign'"
    cnt: campaign
    concurrency-level: 3
    print: "::{{ .json }}"
  # count rows for file for ${LEAS_CAB_CAMPAIGN}
  - cmd: select
    title: count rows for file for ${LEAS_CAB_CAMPAIGN}
    query: "select count(1) as count from c where c.pkey = '${id}'"
    context-query: "select * from c where c.pkey = 'cos-text-file' and STARTSWITH(c.id, '${LEAS_CAB_CAMPAIGN}')"
    cnt: files
    concurrency-level: 3
    print: "::{{ .json }}"
```

and the referenced `lks-sample.yml` looks like..

```
cos-name: "default"
endpoint: "${LEAS_CAB_COSDB_ENDPOINT}"
account-key: "${LEAS_CAB_COSDB_ACCTKEY}"
db:
  id: "leas_cab_db"
  name: "${LEAS_CAB_COSDB_DBNAME}"
collections:
  - id: campaign
    name: tokens
  - id: tokens
    name: tokens
  - id: bearers
    name: tokens
  - id: files
    name: tokens
```

You note two things:

- you can use env vars to customize the values in the files
- the `lks-sample.yml` provides you with  a value of indirection and can help in resolving differences of environment (i.e. the naming of the actual containers and the db itself)

Below an example of the output of the sample script provided.

```
# -title "count campaign" 
# -cmd select -cnt "tokens" -query "select count(1) as count from c where c.pkey = 'campaign'" -print "::{{ .json }}" 
::{"count":1}
# ----------------------- 
# -title "count rows for file for BPMGM1" 
# -cmd select -cnt "tokens" -query "select count(1) as count from c where c.pkey = '${id}'" -print "::{{ .json }}" -context-query "select * from c where c.pkey = 'cos-text-file' and STARTSWITH(c.id, 'BPMGM1')" 
::{"count":10}
::{"count":10}
# ----------------------- 
```