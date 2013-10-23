package org.icatproject.ids.client;

import java.net.URL;

import org.junit.Test;

/** This package has no Javadoc associated as it is only for testing */
public class ClientTest {

	private final static String urlString = "https://smfisher.esc.rl.ac.uk:8181/";

	@Test
	public void testNew() throws Exception {
		new IdsClient(new URL(urlString));
	}

	@Test(expected = BadRequestException.class)
	public void testGetStatus() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.getStatus("");
	}
	
	@Test(expected = BadRequestException.class)
	public void testGetStatus2() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.getStatus("", new DataSelection());
	}
	
	@Test
	public void testPing() throws Exception {
		IdsClient client = new IdsClient(new URL(urlString));
		client.ping();
	}

}