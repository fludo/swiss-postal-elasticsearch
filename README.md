# Swiss Postal Addresses loading to ElasticSearch
Load all swiss postal adresses from [Swiss Post](http://www.post.ch) in an ElasticSearch server instance using Python Panda library and the Elastic Search client.

**No warranty for completeness and correctness! Use at your own risk.**
  
## Usage
 1. Download the latest post_adressdaten from https://service.post.ch/zopa/dlc/app/#!/main
 2. Place it in the root directory. In swisspostal-adresses_panda.py, change the filename variable value accordingly and set the correct server adress/credentials in the code.
 3. Choose an index name (by default 'swiss-streets')
 4. Create the index in elastic search: `PUT swiss-streets` and the mapping `PUT swiss-streets/_mapping
{... <see mapping.txt> ...}`
 5. Execute swisspostal-adresses_panda.py, after a few minutes, you should have the 1.8Mio adresses loaded with raw column names from Swiss Post file.

In ElasticSearch, your can now search adresses, for example using:

   
     GET swiss-streets/_search{
      "query":{
        "multi_match":{
          "query": "Rue 12 Lac Lausanne",
          "fields": ["STR_BEZ_2K","PLZ","ORT_BEZ_18","HNR"],
          "type": "most_fields"
        }
      },
      "size": 400
    }
## Limitations
This code illustrates the process of converting the Swiss post adresses file into a Panda dataframe and inserting it into Elastic Search.
It has the following limitations
- Not all record types are handled or inserted into the ES index
- The python code assumes you have enough RAM to run it in memory. Otherwise it will fail.
- No id is specified, therefore ES use a generated one. This is a problem to maintain records in the index.

## Author
Code by Ludovic Favre 

