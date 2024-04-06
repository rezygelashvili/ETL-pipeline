## Pipeline for extracting ethereum nft collections from OpenSea API and storing them in PostgreSQL.

CustomORM is a class that provides a simple and intuitive way to interact with PostgreSQL databases using an object-relational mapping (ORM) approach. With CustomORM, you can easily perform common database operations such as creating and dropping tables, inserting and selecting data, and more, without having to write complex SQL queries. ETLPipeline extracts the data from OpenSea API and takes advantage of CustomORM to load it into a PostgreSQL database.

Get your API key -> https://docs.opensea.io/reference/api-keys

Create a .env file to use your own API key and pgAdmin credentials.
