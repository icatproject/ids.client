package org.icatproject.ids.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;

import org.icatproject.ids.client.IdsClient.Flag;
import org.icatproject.ids.client.IdsClient.ServiceStatus;
import org.icatproject.ids.integration.BaseTest;
import org.junit.Test;

public abstract class Common extends BaseTest {

	protected static IdsClient client;

	/** This is to stop eclipse trying to treat this class as a unit test */
	public abstract void enable();

	@Test
	public void testGetServiceStatus() throws Exception {
		ServiceStatus status = (client.getServiceStatus(sessionId));
		assertTrue(status.getOpItems().isEmpty());
		assertTrue(status.getLockedDs().isEmpty());
		assertEquals(0, status.getLockCount());
	}

	@Test
	public void testGetApiVersion() throws Exception {
		assertTrue(client.getApiVersion().startsWith("1.3."));
	}

	@Test(expected = NotFoundException.class)
	public void testGetStatus() throws Exception {
		client.getStatus(sessionId, new DataSelection().addDatafile(42L));
	}



	@Test(expected = NotFoundException.class)
	public void testGetData() throws Exception {
		client.getData(sessionId, new DataSelection().addDatafile(42L), Flag.NONE, null, 0);
	}

	@Test(expected = NotFoundException.class)
	public void testGetLink() throws Exception {
		client.getLink(sessionId, 42L);
	}

	@Test
	public void testGetDataUrl() {
		URL url = client.getDataUrl(
				sessionId,
				new DataSelection().addDatasets(Arrays.asList(1L, 2L))
						.addInvestigations(Arrays.asList(3L, 4L)).addDatafile(42L),
				Flag.ZIP_AND_COMPRESS, "my favourite name");
		assertEquals(setup.getIdsUrl().getHost(), url.getHost());
		assertEquals(setup.getIdsUrl().getPort(), url.getPort());
		assertEquals("/ids/getData", url.getPath());
		assertEquals(setup.getIdsUrl().getProtocol(), url.getProtocol());
		assertTrue(url.getQuery().contains("sessionId=" + sessionId));
		assertTrue(url.getQuery().contains("compress=true"));
		assertTrue(url.getQuery().contains("zip=true"));
		assertTrue(url.getQuery().contains("outname=my+favourite+name"));
		assertTrue(url.getQuery().contains("investigationIds=3%2C4"));
		assertTrue(url.getQuery().contains("datafileIds=42"));
		assertTrue(url.getQuery().contains("datasetIds=1%2C2"));
	}

	@Test
	public void testGetDataUrl2() {
		URL url = client.getDataUrl(sessionId, "my favourite name");
		assertEquals(setup.getIdsUrl().getHost(), url.getHost());
		assertEquals(setup.getIdsUrl().getPort(), url.getPort());
		assertEquals("/ids/getData", url.getPath());
		assertEquals(setup.getIdsUrl().getProtocol(), url.getProtocol());
		assertTrue(url.getQuery().contains("preparedId=" + sessionId));
		assertTrue(url.getQuery().contains("outname=my+favourite+name"));
	}

	@Test
	public void testGetDataUrl3() {
		URL url = client.getDataUrl(sessionId, null);
		assertEquals(setup.getIdsUrl().getHost(), url.getHost());
		assertEquals(setup.getIdsUrl().getPort(), url.getPort());
		assertEquals("/ids/getData", url.getPath());
		assertEquals(setup.getIdsUrl().getProtocol(), url.getProtocol());
		assertTrue(url.getQuery().contains("preparedId=" + sessionId));
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
		is.close();
	}

	@Test(expected = NotFoundException.class)
	public void testPut2() throws Exception {
		InputStream is = Files.newInputStream(new File("a.b").toPath());
		client.put(sessionId, is, "fred", 1L, 2L, "Description", null, null, null);
		is.close();
	}

}