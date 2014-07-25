capitol_words_nlp
=================

Experimenting with parsing the congressional record using NLP techniques and tools

Setup
-----

1. install deps

```
pip install -r requirements.txt
```

2. download [Stanford CoreNLP v1.3.4](http://nlp.stanford.edu/software/stanford-corenlp-full-2012-11-12.zip) and unzip it to /opt/

Usage
-----

First, get mongo running. Then:

```
python import_to_mongo.py path_to_data_dir desired_name_of_mongo_db
```
