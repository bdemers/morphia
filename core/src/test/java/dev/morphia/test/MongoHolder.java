package dev.morphia.test;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import org.bson.UuidRepresentation;
import org.testcontainers.containers.MongoDBContainer;

import static com.mongodb.MongoClientSettings.builder;

class MongoHolder implements AutoCloseable {
    private MongoDBContainer mongoDBContainer;
    private String connectionString;
    private MongoClient mongoClient;

    public MongoHolder(MongoDBContainer mongoDBContainer, String connectionString) {
        this.mongoDBContainer = mongoDBContainer;
        this.connectionString = connectionString;
    }

    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (isAlive()) {
            mongoDBContainer.close();
        }
    }

    public boolean isAlive() {
        return mongoDBContainer != null && mongoDBContainer.isRunning();
    }

    public MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(builder()
                    .uuidRepresentation(UuidRepresentation.STANDARD)
                    .applyConnectionString(new ConnectionString(connectionString))
                    .build());

        }
        return mongoClient;
    }

    String connectionString(String dbName) {
        if (mongoDBContainer != null) {
            return mongoDBContainer.getReplicaSetUrl(dbName);
        } else {
            return "mongodb://localhost:27017/" + dbName;
        }
    }
}
