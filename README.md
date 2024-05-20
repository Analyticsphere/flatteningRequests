# flatteningRequests

A place to communicate about the team flattened data needs.

Use the *issues* tools to make and manage requests.

## The flattening workflow:
```mermaid
flowchart LR
    subgraph BQ [BigQuery]
        SRC[[Source Table]]:::table
        QUERY(Scheduled Query):::table
        FLAT[[Flattened Table]]:::table
    end
    class BQ subgraphTitle;

    SCH[/schema.json/]:::file
    DEV[/dev.sql/]:::file
    STG[/stg.sql/]:::file
    PROD[/prod.sql/]:::file

    subgraph LOC ["flatten.Rmd"]
        FETCH("fetch_schema()"):::function
        SCH_CSV[/"schema.csv"/]:::file
        FILT("sort_variables()"):::function
        ARR[/arrays.json/]:::file
        VAR[/variables.csv/]:::file
        GEN("generate_queries()"):::function
    end
    class LOC subgraphTitle;

    SRC --- |API| SCH:::link
    SCH --> FETCH:::link
    FETCH --> SCH_CSV:::link
    SCH_CSV --> FILT:::link
    FILT --> ARR:::link & VAR:::link
    ARR & VAR --> GEN:::link
    GEN --> DEV:::link & STG:::link & PROD:::link
    DEV & STG & PROD --> |API| QUERY:::link
    QUERY --> FLAT:::link

    classDef table link stroke:#000,stroke-width:1.5px;
    classDef schema fill:#ff9,stroke:#333,stroke-width:2px;
    classDef function fill:#ffc,stroke:#333,stroke-width:2px;
    classDef file fill:#cfc,stroke:#333,stroke-width:2px;
    classDef subgraphTitle font-size:24px;
```

`flatten.Rmd` is an R notebook that simplifies the process of flattening the Connect4Cancer source tables:
1. Retrieve the schema of the source table using the BigQuery API.
2. Convert the schema from JSON to CSV.
3. Sort the variables in the source table by their data structure: arrays and non-array variables and stores them in appropriate files.
4. Generate SQL queries for each environment.
5. Update the Scheduled Query which generates the flattened table daily.

> **Notes:**
> - The user can ignore the `bash/`, `python/` and `js/` directories which store legacy code that could be referenced later.
> - The core functions of this workflow are in teh `r/` directory

## Query configurations
The `query_configs/` directory stores a metadata for each table to be flattened and is referenced by the `flatten.Rmd` notebook. To set up a new table for flattening, a new config folder needs to be setup for the new table with meta data about the table. 

Here is an example:

```

```
