config = {"solr": {
              "store_name": {
                 "host": "Host where Solr core is located for this particular store"
                 }
              },
              "redshift": {
                  "skus_query_path": "path/to/redshift/select.sql",
                  "store_name": {
                      "conn": {
                          "host": "Redshift hostname",
                          "dbname": "database name",
                          "port": "port, int",
                          "user": "user name",
                          "password": "password"
                      }
                  }
              },
              "gcp": {
                  "json_path": "path/to/key.json",
                  "bigquery": {
                      "table": "table where data will be saved.",
                      "dataset": "dataset where table is located"
                  },
                  "storage": {
                      "bucket": "bucket to save results to",
                      "blobl": "blob to receive the data"  
                  }
              }
          }
