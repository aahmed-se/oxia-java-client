package oxia.java.client;

import org.testcontainers.containers.GenericContainer;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OxiaKVClientTest {

    private GenericContainer<?> container;

    @BeforeTest
    public void startContainer() {
        container = new GenericContainer<>("streamnative/oxia")
                .withCommand("oxia", "standalone")
                .withExposedPorts(6648);
        container.start();
    }

    @AfterTest
    public void stopContainer() {
        container.stop();
    }

    OxiaKVClient client() {
        return new OxiaKVClient(container.getHost(), container.getMappedPort(6648));
    }

    @Test
    public void testPutAndGet() {
        OxiaKVClient client = client();
        String key = "test-key";
        byte[] value = "test-value".getBytes();

        client.put(key, value);
        byte[] retrievedValue = client.get(key);
        Assert.assertEquals("test-value", new String(retrievedValue));
    }


}
