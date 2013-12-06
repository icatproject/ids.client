package org.icatproject.ids.client;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;

import org.icatproject.ids.client.IdsClient.Flag;
import org.icatproject.ids.client.IdsClient.ServiceStatus;
import org.icatproject.ids.integration.BaseTest;
import org.icatproject.ids.integration.util.Setup;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClientTest extends BaseTest {

	private static IdsClient client;

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("one.properties");
		icatsetup();
		URL url = new URL(setup.getIdsUrl(), "/");
		client = new IdsClient(url);

		OutputStream p = Files.newOutputStream(new File("a.b").toPath());
		p.write("wibble".getBytes());
		byte[] bytes = new byte[100000];
		p.write(bytes);
		p.close();
	}

	@Test
	public void testGetServiceStatus() throws Exception {
		ServiceStatus status = (client.getServiceStatus(sessionId));
		assertTrue(status.getOpItems().isEmpty());
		assertTrue(status.getPrepItems().isEmpty());
	}

	@Test(expected = BadRequestException.class)
	public void testGetStatus() throws Exception {
		client.getStatus(sessionId, new DataSelection());
	}

	@Test(expected = BadRequestException.class)
	public void testRestore() throws Exception {
		client.restore(sessionId, new DataSelection());
	}

	@Test(expected = BadRequestException.class)
	public void testArchive() throws Exception {
		client.archive(sessionId, new DataSelection());
	}

	@Test(expected = BadRequestException.class)
	public void testGetData() throws Exception {
		client.getData(sessionId, new DataSelection(), Flag.NONE, null, 0);
	}

	@Test(expected = NotFoundException.class)
	public void testIsPrepared() throws Exception {
		client.isPrepared(sessionId);
	}

	@Test(expected = NotFoundException.class)
	public void testGetData2() throws Exception {
		client.getData(sessionId, null, 0);
	}

	@Test
	public void testPing() throws Exception {
		client.ping();
	}

	@Test(expected = NotFoundException.class)
	public void testPut() throws Exception {
		InputStream is = Files.newInputStream(new File("a.b").toPath());
		client.put(sessionId, is, "fred", 1L, 2L, "Description");
	}

	@Test(expected = NotFoundException.class)
	public void testPut2() throws Exception {
		InputStream is = Files.newInputStream(new File("a.b").toPath());
		client.put(sessionId, is, "fred", 1L, 2L, "Description", null, null, null);
	}

}