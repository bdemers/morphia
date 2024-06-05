package dev.morphia.test;

import java.util.List;
import java.util.stream.Collectors;

import com.github.zafarkhaja.semver.Version;
import com.mongodb.client.MongoClient;

import dev.morphia.config.MorphiaConfig;
import dev.morphia.mapping.Mapper;
import dev.morphia.test.TestBase.ZDTCodecProvider;
import dev.morphia.test.config.ManualMorphiaTestConfig;
import dev.morphia.test.config.MorphiaTestConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;

import static java.lang.String.format;
import static java.util.Arrays.stream;

@SuppressWarnings("removal")
public class MorphiaTestSetup {
    private static final Logger LOG = LoggerFactory.getLogger(MorphiaTestSetup.class);

    /**
     * Suite level mongo holder used to manage Mongo Test Container.
     */
    private static MongoHolder suiteMongoHolder;

    /**
     * Class level mongo holder with unique db/connection string per class.
     */
    private MongoHolder classMongoHolder;

    private MorphiaContainer morphiaContainer;
    private MorphiaConfig morphiaConfig;
    private final String testDBName;

    public MorphiaTestSetup() {
        this(buildConfig()
                .codecProvider(new ZDTCodecProvider()));
    }

    public MorphiaTestSetup(MorphiaConfig config) {
        // generate a unique db name for this test class (max of 63 chars)
        String testClassName = getClass().getName().replaceAll("\\.", "-");
        if (testClassName.length() > 63) {
            testClassName = testClassName.substring(testClassName.length() - 63);
        }
        testDBName = testClassName;
        morphiaConfig = config.database(testClassName);
    }

    protected String getTestDBName() {
        return testDBName;
    }

    @BeforeSuite
    public void setupMorphiaTestContainer() {
        suiteMongoHolder = initMongoDbContainer(false, getTestDBName());
    }

    @BeforeClass
    public void setupConnections() {
        morphiaContainer = new MorphiaContainer(suiteMongoHolder.getMongoClient(), morphiaConfig);
        classMongoHolder = new MongoHolder(null, suiteMongoHolder.connectionString(getTestDBName()));
    }

    public MorphiaContainer getMorphiaContainer() {
        return morphiaContainer;
    }

    public MongoHolder getMongoHolder() {
        return classMongoHolder;
    }

    private static MongoHolder initMongoDbContainer(boolean sharded, String dbName) {
        String mongodb = System.getProperty("mongodb");
        String connectionString;
        MongoDBContainer mongoDBContainer = null;
        if ("local".equals(mongodb)) {
            LOG.info("'local' mongodb property specified. Using local server.");
            connectionString = "mongodb://localhost:27017/" + dbName;
        } else {
            DockerImageName imageName;
            try {
                Versions match = mongodb == null
                        ? Versions.latest()
                        : Versions.bestMatch(mongodb);
                imageName = match.dockerImage();
            } catch (IllegalArgumentException e) {
                imageName = Versions.latest().dockerImage();
                LOG.error(format("Could not parse mongo docker image name.  using docker image %s.", imageName));
            }

            LOG.info("Running tests using " + imageName);
            mongoDBContainer = new MongoDBContainer(imageName);
            if (sharded) {
                mongoDBContainer
                        .withSharding();
            }
            mongoDBContainer.start();
            connectionString = mongoDBContainer.getReplicaSetUrl(dbName);
        }
        return new MongoHolder(mongoDBContainer, connectionString);
    }

    @AfterClass
    public void stopHolder() {
        if (classMongoHolder != null) {
            classMongoHolder.close();
        }
        classMongoHolder = null;
    }

    @AfterSuite
    public void stopContainer() {
        if (suiteMongoHolder != null) {
            suiteMongoHolder.close();
        }
    }

    protected void assumeTrue(boolean condition, String message) {
        if (!condition) {
            throw new SkipException(message);
        }
    }

    protected void checkMinDriverVersion(DriverVersion version) {
        String property = System.getProperty("driver.version");
        Version driverVersion = property != null ? Version.valueOf(property) : null;

        assumeTrue(driverVersion == null || driverVersion.greaterThanOrEqualTo(version.version()),
                format("Server should be at least %s but found %s", version, getServerVersion()));
    }

    protected void checkMinServerVersion(ServerVersion version) {
        assumeTrue(serverIsAtLeastVersion(version.version()),
                format("Server should be at least %s but found %s", version.version(), getServerVersion()));
    }

    protected MongoClient getMongoClient() {
        return getMongoHolder().getMongoClient();
    }

    protected Version getServerVersion() {
        return morphiaContainer.getServerVersion();
    }

    protected boolean isReplicaSet() {
        return morphiaContainer.runIsMaster().get("setName") != null;
    }

    /**
     * @param version the minimum version allowed
     * @return true if server is at least specified version
     */
    protected boolean serverIsAtLeastVersion(Version version) {
        return getServerVersion().greaterThanOrEqualTo(version);
    }

    protected void withSharding(Runnable body) {
        var oldContainer = morphiaContainer;
        try (var holder = initMongoDbContainer(true, getTestDBName())) {
            morphiaContainer = new MorphiaContainer(holder.getMongoClient(), MorphiaConfig.load());
            body.run();
        } finally {
            morphiaContainer = oldContainer;
        }
    }

    private void map(List<Class<?>> classes) {
        Mapper mapper = getMorphiaContainer().getDs().getMapper();
        classes.forEach(mapper::getEntityModel);
    }

    protected void withTestConfig(MorphiaConfig config, List<Class<?>> types, Runnable body) {
        withConfig(new ManualMorphiaTestConfig(config).classes(types), body);
    }

    protected void withTestConfig(List<Class<?>> types, Runnable body) {
        withTestConfig(buildConfig(), types, body);
    }

    protected void withConfig(MorphiaConfig config, Runnable body) {
        var oldContainer = morphiaContainer;
        var oldHolder = classMongoHolder;
        try {
            config = config.database(getTestDBName());
            morphiaContainer = new MorphiaContainer(classMongoHolder.getMongoClient(), config);
            if (config instanceof MorphiaTestConfig testConfig) {
                List<Class<?>> classes = testConfig.classes();
                if (classes != null) {
                    getMorphiaContainer().getDs().getMapper().map(classes);
                }
                if (config.applyIndexes()) {
                    getMorphiaContainer().getDs().applyIndexes();
                }
            }
            body.run();
        } finally {
            morphiaContainer = oldContainer;
            classMongoHolder = oldHolder;
        }
    }

    protected static MorphiaConfig buildConfig(Class<?>... types) {
        MorphiaConfig config = new ManualMorphiaTestConfig();
        if (types.length != 0)
            config = config
                    .packages(stream(types)
                            .map(Class::getPackageName)
                            .collect(Collectors.toList()));
        return config;
    }
}
