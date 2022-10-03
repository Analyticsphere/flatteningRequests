### Goals for Automating Queries

An ideal workflow for generating these queries would generate these queries directly from the table schemas. This would involve:

1.  Downloading a schema as a JSON file

    -   from command line using *gcloud*:

        -   `bq show --schema --format=prettyjson my_gcp_project:my_database.my_table > "my_file.json"`

    -   from R:

        -    see *get_bq_table_schema_as_json.sh*

2.  Loading the JSON into a tree object, storing parent/child relationships and data types.

    -   [treelib](https://treelib.readthedocs.io/en/latest/) would be great for this.

3.  Using the tree to generate the d_CID1_d\_CID2_d\_CIDN\_.. names

4.  Identifying which variables are simply nested and which contain JSON arrays

5.  Add them to the input files based on nesting type.
