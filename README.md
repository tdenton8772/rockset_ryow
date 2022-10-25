# Rockset Java Read Your Own Writes Example

This is an example of how to combine the Rockset WriteAPI and the FencesAPI to read your own writes. 

## Requirements
- Maven
- Java 11 (may run with Java 8 but that is untested)

## Usage

Edit the `configuration.properties` file for your environment. Remember, this is an example and you should not store API Keys in plain text files for production environments.

```bash
API_SERVER=<API Server>
API_KEY=<APIKEY>
workspace=<Workspace that contains your collection>
collection=<collection to write to>
write_threads=<number of threads to start writing>
body_length=<size of document body>
```

After those files are updated you car build and run the JAR file.

```bash
mvn clean package
java -jar ./target/example_write-1.0-SNAPSHOT.jar
```