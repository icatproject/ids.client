package org.icatproject.ids.client;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;

import org.icatproject.ids.integration.util.Setup;
import org.icatproject.utils.ShellCommand;
import org.junit.BeforeClass;
import org.junit.Test;

public class OneTest extends Common {

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
	public void testRestore() throws Exception {
		client.restore(sessionId, new DataSelection().addDatafile(42L));
	}

	@Test
	public void testArchive() throws Exception {
		client.archive(sessionId, new DataSelection().addDatafile(42L));
	}

	@Test
	public void python() {
		ShellCommand sc = new ShellCommand("bash", "-c",
				"PYTHONPATH=src/main/python/ python src/test/python/ClientTest.py 1");
		if (sc.getExitValue() != 0) {
			System.out.println(sc.getStdout());
			System.out.println(sc.getStderr());
			fail();
		}
	}

	@Override
	public void enable() {
		// Only needs to be here
	}
}