package org.icatproject.ids.client;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;

import org.icatproject.ids.client.IdsClient.Flag;
import org.icatproject.ids.integration.BaseTest;
import org.junit.Test;

public class ClientTest extends BaseTest {

	private final static String urlString = "https://smfisher.esc.rl.ac.uk:8181/";

	@Test
	public void testNew() throws Exception {
		new IdsClient(new URL(urlString));
	}

	@Test(expected = InsufficientPrivilegesException.class)
	public void testGetServiceStatus() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.getServiceStatus("");
	}

	@Test(expected = BadRequestException.class)
	public void testGetStatus() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.getStatus("", new DataSelection());
	}

	@Test(expected = BadRequestException.class)
	public void testRestore() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.restore("", new DataSelection());
	}

	@Test(expected = BadRequestException.class)
	public void testArchive() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.archive("", new DataSelection());
	}

	@Test(expected = BadRequestException.class)
	public void testGetData() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.getData("", new DataSelection(), Flag.NONE, null, 0);
	}

	@Test(expected = BadRequestException.class)
	public void testGetData2() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.getData("", null, 0);
	}

	@Test
	public void testPing() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.ping();
	}

	@Test
	public void testPut() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.put("", Files.newInputStream(new File("/tmp/t.two").toPath()), "fred", 1L, 2L,
				"Description");
	}

	@Test
	public void testPut2() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.put("", Files.newInputStream(new File("/tmp/t.two").toPath()), "fred", 1L, 2L,
				"Description", null, null, null);
	}

}