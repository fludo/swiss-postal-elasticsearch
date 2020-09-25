# Swiss Postal Addresses loading to ElasticSearch
Load all swiss postal adresses from [Swiss Post](http://www.post.ch) in an ElasticSearch server instance using Python Panda library and the Elastic Search client.
**No warrenty for completeness and correctness! Use at your own risk.**
  
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


## Author
Code by Ludovic Favre 

