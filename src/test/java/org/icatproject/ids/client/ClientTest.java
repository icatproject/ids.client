package org.icatproject.ids.client;

import static org.junit.Assert.assertTrue;

import java.net.URL;

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

	@Test(expected = BadRequestException.class)
	public void testPut() throws Exception {
		client.put(sessionId, null, "fred", 1L, 2L, "Description");
	}

	@Test(expected = BadRequestException.class)
	public void testPut2() throws Exception {
		client.put(sessionId, null, "fred", 1L, 2L, "Description", null, null, null);
	}

}