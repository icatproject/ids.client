package org.icatproject.ids.client;

import java.io.File;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;

import org.icatproject.ids.integration.util.Setup;
import org.junit.BeforeClass;

public class TwoTest extends Common {

	@BeforeClass
	public static void setup() throws Exception {
		setup = new Setup("two.properties");
		icatsetup();
		URL url = new URL(setup.getIdsUrl(), "/");
		client = new IdsClient(url);

		OutputStream p = Files.newOutputStream(new File("a.b").toPath());
		p.write("wibble".getBytes());
		byte[] bytes = new byte[100000];
		p.write(bytes);
		p.close();
	}

	@Override
	public void enable() {
		// Only needs to be here
	}
}