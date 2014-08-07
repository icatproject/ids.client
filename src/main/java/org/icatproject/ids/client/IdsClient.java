package org.icatproject.ids.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Client to communicate with IDS server
 */
public class IdsClient {

	/**
	 * Defines packaging options
	 */
	public enum Flag {
		/**
		 * Apply compression if the file or files are zipped.
		 */
		COMPRESS,

		/**
		 * No zipping when a single data file is requested and no compression.
		 */
		NONE,

		/**
		 * Also zip when a single data file is requested.
		 */
		ZIP,

		/**
		 * Compress and also zip when a single data file is requested.
		 */
		ZIP_AND_COMPRESS
	}

	/**
	 * Returned by the getServiceStatus call
	 */
	public class ServiceStatus {

		private Map<String, String> opItems = new HashMap<>();
		private Map<String, String> prepItems = new HashMap<>();

		/**
		 * Return a map from a description of a data set to the requested state.
		 * 
		 * @return map
		 */
		public Map<String, String> getOpItems() {
			return opItems;
		}

		/**
		 * Return a map from a preparedId to the state of the preparer.
		 * 
		 * @return map
		 */
		public Map<String, String> getPrepItems() {
			return prepItems;
		}

		void storeOpItems(String dsInfo, String request) {
			opItems.put(dsInfo, request);
		}

		void storePrepItems(String id, String state) {
			prepItems.put(id, state);
		}

	};

	/**
	 * Values to be returned by the {@code getStatus()} calls.
	 */
	public enum Status {
		/**
		 * When some or all of the requested data are not on line and restoration has not been
		 * requested
		 */
		ARCHIVED,

		/**
		 * All requested data are on line.
		 */

		ONLINE,

		/**
		 * When some or all of the requested data are not on line but otherwise restoration has been
		 * requested.
		 */
		RESTORING
	};

	private String basePath;

	private URI idsUri;

	private URL idsUrl;

	/**
	 * @param idsUrl
	 *            The URL of the ids server host. This should be of the form
	 *            https://example.com:443.
	 */
	public IdsClient(URL idsUrl) {
		try {
			basePath = idsUrl.getFile();
			if (!basePath.endsWith("/")) {
				basePath = basePath + "/";
			}
			basePath = basePath + "ids/";
			String protocol = idsUrl.getProtocol();
			String host = idsUrl.getHost();
			int port = idsUrl.getPort();

			this.idsUri = new URI(protocol, null, host, port, null, null, null);
			this.idsUrl = new URL(protocol, host, port, basePath);
		} catch (URISyntaxException | MalformedURLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Archive data specified by the dataSelection.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
	 * 
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws InternalException
	 * @throws NotFoundException
	 */
	public void archive(String sessionId, DataSelection dataSelection)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotFoundException {

		URI uri = getUri(getUriBuilder("archive"));
		List<NameValuePair> formparams = new ArrayList<>();
		formparams.add(new BasicNameValuePair("sessionId", sessionId));
		for (Entry<String, String> entry : dataSelection.getParameters().entrySet()) {
			formparams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
		}
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpPost httpPost = new HttpPost(uri);
			httpPost.setEntity(new UrlEncodedFormEntity(formparams));
			try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
				expectNothing(response);
			} catch (InsufficientStorageException | DataNotOnlineException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	private void checkStatus(HttpResponse response) throws InternalException, BadRequestException,
			DataNotOnlineException, ParseException, IOException, InsufficientPrivilegesException,
			NotImplementedException, InsufficientStorageException, NotFoundException {
		StatusLine status = response.getStatusLine();
		if (status == null) {
			throw new InternalException("Status line returned is empty");
		}
		int rc = status.getStatusCode();
		if (rc / 100 != 2) {
			HttpEntity entity = response.getEntity();
			String error;
			if (entity == null) {
				throw new InternalException("No explanation provided");
			} else {
				error = EntityUtils.toString(entity);
			}
			String code;
			String message;
			try {
				ObjectMapper om = new ObjectMapper();
				JsonNode rootNode = om.readValue(error, JsonNode.class);
				code = rootNode.get("code").asText();
				message = rootNode.get("message").asText();
			} catch (Exception e) {
				throw new InternalException("Status code " + rc
						+ " returned but message not json: " + error);
			}

			if (code.equals("BadRequestException")) {
				throw new BadRequestException(message);
			}

			if (code.equals("DataNotOnlineException")) {
				throw new DataNotOnlineException(message);
			}

			if (code.equals("InsufficientPrivilegesException")) {
				throw new InsufficientPrivilegesException(message);
			}

			if (code.equals("InsufficientStorageException")) {
				throw new InsufficientStorageException(message);
			}

			if (code.equals("InternalException")) {
				throw new InternalException(message);
			}

			if (code.equals("NotFoundException")) {
				throw new NotFoundException(message);
			}

			if (code.equals("NotImplementedException")) {
				throw new NotImplementedException(message);
			}
		}

	}

	/**
	 * Delete data specified by the dataSelection.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
	 * 
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws InternalException
	 * @throws NotFoundException
	 * @throws DataNotOnlineException
	 */
	public void delete(String sessionId, DataSelection dataSelection)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotFoundException, DataNotOnlineException {

		URIBuilder uriBuilder = getUriBuilder("delete");
		uriBuilder.addParameter("sessionId", sessionId);
		for (Entry<String, String> entry : dataSelection.getParameters().entrySet()) {
			uriBuilder.addParameter(entry.getKey(), entry.getValue());
		}
		URI uri = getUri(uriBuilder);

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpDelete httpDelete = new HttpDelete(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpDelete)) {
				expectNothing(response);
			} catch (InsufficientStorageException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	private void expectNothing(CloseableHttpResponse response) throws InternalException,
			BadRequestException, DataNotOnlineException, ParseException,
			InsufficientPrivilegesException, NotImplementedException, InsufficientStorageException,
			NotFoundException, IOException {
		checkStatus(response);
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			if (!EntityUtils.toString(entity).isEmpty()) {
				throw new InternalException("No http entity expected in response");
			}
		}
	}

	/**
	 * Get the data specified by the dataSelection.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
	 * @param flags
	 *            To select packing options
	 * @param outname
	 *            The name of the file. If it is in .zip format the .zip extension will be added if
	 *            not present.
	 * @param offset
	 *            Skip this number of bytes in the returned stream
	 * 
	 * @return an InputStream to allow the data to be read. Please remember to close the stream when
	 *         you have finished with it.
	 * 
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws NotFoundException
	 * @throws InternalException
	 * @throws DataNotOnlineException
	 */
	public InputStream getData(String sessionId, DataSelection dataSelection, Flag flags,
			String outname, long offset) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, NotFoundException, InternalException,
			DataNotOnlineException {
		URIBuilder uriBuilder = getUriBuilder("getData");
		uriBuilder.setParameter("sessionId", sessionId);
		for (Entry<String, String> entry : dataSelection.getParameters().entrySet()) {
			uriBuilder.setParameter(entry.getKey(), entry.getValue());
		}

		if (flags == Flag.ZIP || flags == Flag.ZIP_AND_COMPRESS) {
			uriBuilder.setParameter("zip", "true");
		}
		if (flags == Flag.COMPRESS || flags == Flag.ZIP_AND_COMPRESS) {
			uriBuilder.setParameter("compress", "true");
		}
		if (outname != null) {
			uriBuilder.setParameter("outname", outname);
		}
		URI uri = getUri(uriBuilder);
		CloseableHttpResponse response = null;
		CloseableHttpClient httpclient = null;
		HttpGet httpGet = new HttpGet(uri);
		if (offset != 0) {
			httpGet.setHeader("Range", "bytes=" + offset + "-");
		}
		boolean closeNeeded = true;
		try {
			httpclient = HttpClients.createDefault();
			response = httpclient.execute(httpGet);
			checkStatus(response);
			closeNeeded = false;
			return new HttpInputStream(httpclient, response);
		} catch (IOException | InsufficientStorageException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		} finally {
			if (closeNeeded && httpclient != null) {
				try {
					if (response != null) {
						try {
							response.close();
						} catch (Exception e) {
							// Ignore it
						}
					}
					httpclient.close();
				} catch (IOException e) {
					// Ignore it
				}
			}
		}

	}

	/**
	 * Get the data using the preparedId returned by a call to prepareData
	 * 
	 * @param preparedId
	 *            A valid preparedId returned by a call to prepareData
	 * 
	 * @param outname
	 *            The name of the file. If it is in .zip format the .zip extension will be added if
	 *            not present.
	 * @param offset
	 *            Skip this number of bytes in the returned stream
	 * 
	 * @return an InputStream to allow the data to be read. Please remember to close the stream when
	 *         you have finished with it.
	 * 
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws NotFoundException
	 * @throws InternalException
	 * @throws DataNotOnlineException
	 */
	public InputStream getData(String preparedId, String outname, long offset)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			NotFoundException, InternalException, DataNotOnlineException {
		URIBuilder uriBuilder = getUriBuilder("getData");
		uriBuilder.setParameter("preparedId", preparedId);
		if (outname != null) {
			uriBuilder.setParameter("outname", outname);
		}
		URI uri = getUri(uriBuilder);

		CloseableHttpResponse response = null;
		CloseableHttpClient httpclient = null;
		HttpGet httpGet = new HttpGet(uri);
		if (offset != 0) {
			httpGet.setHeader("Range", "bytes=" + offset + "-");
		}
		boolean closeNeeded = true;
		try {
			httpclient = HttpClients.createDefault();
			response = httpclient.execute(httpGet);
			checkStatus(response);
			closeNeeded = false;
			return new HttpInputStream(httpclient, response);
		} catch (IOException | InsufficientStorageException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		} finally {
			if (closeNeeded && httpclient != null) {
				try {
					if (response != null) {
						try {
							response.close();
						} catch (Exception e) {
							// Ignore it
						}
					}
					httpclient.close();
				} catch (IOException e) {
					// Ignore it
				}
			}
		}
	}

	private URL getDataUrl(Map<String, String> parameters) {
		try {
			URL url = new URL(idsUrl, "getData");
			StringBuilder sb = new StringBuilder();
			for (Entry<String, String> e : parameters.entrySet()) {
				if (sb.length() != 0) {
					sb.append("&");
				}
				sb.append(e.getKey() + "=" + URLEncoder.encode(e.getValue(), "UTF-8"));
			}
			url = new URL(url + "?" + sb.toString());
			if (url.toString().length() > 2048) {
				throw new BadRequestException("Generated URL is of length "
						+ url.toString().length() + " which exceeds 2048");
			}
			return url;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Get the URL to retrieve the data specified by the dataSelection.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
	 * @param flags
	 *            To select packing options
	 * @param outname
	 *            The name of the file. If it is in .zip format the .zip extension will be added if
	 *            not present.
	 * 
	 * @return the URL to allow the data to be read
	 */
	public URL getDataUrl(String sessionId, DataSelection dataSelection, Flag flags, String outname) {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(dataSelection.getParameters());
		if (flags == Flag.ZIP || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("zip", "true");
		}
		if (flags == Flag.COMPRESS || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("compress", "true");
		}
		if (outname != null) {
			parameters.put("outname", outname);
		}
		return getDataUrl(parameters);
	}

	/**
	 * Get the URL to retrieve the data specified by the preparedId returned by a call to
	 * prepareData
	 * 
	 * @param preparedId
	 *            A valid preparedId returned by a call to prepareData
	 * 
	 * @param outname
	 *            The name of the file. If it is in .zip format the .zip extension will be added if
	 *            not present.
	 * 
	 * @return the URL to allow the data to be read
	 */
	public URL getDataUrl(String preparedId, String outname) {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("preparedId", preparedId);
		if (outname != null) {
			parameters.put("outname", outname);
		}
		return getDataUrl(parameters);
	}

	/**
	 * Set a hard link to a data file.
	 * 
	 * This is only useful in those cases where the user has direct access to the file system where
	 * the IDS is storing data. The container in which the IDS is running must be allowed to write
	 * the link.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param datafileId
	 *            the id of a data file
	 * @param link
	 *            the absolute path of a link to be set. This will first be deleted if it already
	 *            exists.
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws InternalException
	 * @throws NotFoundException
	 * @throws DataNotOnlineException
	 * @throws NotImplementedException
	 *             if the user does not have direct access to the file system where the IDS is
	 *             storing data
	 */
	public void getLink(String sessionId, long datafileId, Path link) throws BadRequestException,
			InsufficientPrivilegesException, InternalException, NotFoundException,
			DataNotOnlineException, NotImplementedException {
		URI uri = getUri(getUriBuilder("getLink"));
		List<NameValuePair> formparams = new ArrayList<>();
		formparams.add(new BasicNameValuePair("sessionId", sessionId));
		formparams.add(new BasicNameValuePair("datafileId", Long.toString(datafileId)));
		formparams.add(new BasicNameValuePair("link", link.toString()));
		formparams.add(new BasicNameValuePair("username", System.getProperty("user.name")));

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpPost httpPost = new HttpPost(uri);
			httpPost.setEntity(new UrlEncodedFormEntity(formparams));
			try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
				expectNothing(response);
			} catch (InsufficientStorageException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Return a ServiceStatus object to understand what the IDS is doing.
	 * 
	 * To use this call, the user represented by the sessionId must be in the set of rootUserNames
	 * defined in the IDS configuration.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID of a user in the IDS rootUserNames set.
	 * 
	 * @return a ServiceStatus object
	 * 
	 * @throws InternalException
	 * @throws InsufficientPrivilegesException
	 * @throws NotImplementedException
	 * @throws
	 */
	public ServiceStatus getServiceStatus(String sessionId) throws InternalException,
			InsufficientPrivilegesException, NotImplementedException {
		URIBuilder uriBuilder = getUriBuilder("getServiceStatus");
		uriBuilder.setParameter("sessionId", sessionId);
		URI uri;
		try {
			uri = getUri(uriBuilder);
		} catch (BadRequestException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);

			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				String result = getString(response);
				ObjectMapper mapper = new ObjectMapper();
				JsonNode rootNode = mapper.readValue(result, JsonNode.class);
				ServiceStatus serviceStatus = new ServiceStatus();
				for (JsonNode on : (ArrayNode) rootNode.get("opsQueue")) {
					String dsInfo = ((ObjectNode) on).get("dsInfo").asText();
					String request = ((ObjectNode) on).get("request").asText();
					serviceStatus.storeOpItems(dsInfo, request);
				}
				for (JsonNode on : (ArrayNode) rootNode.get("prepQueue")) {
					String id = ((ObjectNode) on).get("id").asText();
					String state = ((ObjectNode) on).get("state").asText();
					serviceStatus.storePrepItems(id, state);
				}
				return serviceStatus;
			} catch (InsufficientStorageException | DataNotOnlineException | InternalException
					| BadRequestException | NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Returns size of the datafiles described by the dataSelection. This is not the same as the
	 * size of a zip file containing these datafiles.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object
	 * 
	 * @return the total size in bytes
	 * 
	 * @throws BadRequestException
	 * @throws NotFoundException
	 * @throws InsufficientPrivilegesException
	 * @throws InternalException
	 * @throws NotImplementedException
	 * @throws
	 */
	public long getSize(String sessionId, DataSelection dataSelection) throws BadRequestException,
			NotFoundException, InsufficientPrivilegesException, InternalException,
			NotImplementedException {

		URIBuilder uriBuilder = getUriBuilder("getSize");
		uriBuilder.setParameter("sessionId", sessionId);
		for (Entry<String, String> entry : dataSelection.getParameters().entrySet()) {
			uriBuilder.setParameter(entry.getKey(), entry.getValue());
		}
		URI uri = getUri(uriBuilder);

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return Long.parseLong(getString(response));
			} catch (IOException | InsufficientStorageException | DataNotOnlineException
					| NumberFormatException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Return the status of the data specified by the dataSelection.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
	 * 
	 * @return the status
	 * 
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws NotFoundException
	 * @throws InternalException
	 */
	public Status getStatus(String sessionId, DataSelection dataSelection)
			throws BadRequestException, NotFoundException, InsufficientPrivilegesException,
			InternalException, NotImplementedException {
		URIBuilder uriBuilder = getUriBuilder("getStatus");
		uriBuilder.setParameter("sessionId", sessionId);
		for (Entry<String, String> entry : dataSelection.getParameters().entrySet()) {
			uriBuilder.addParameter(entry.getKey(), entry.getValue());
		}
		URI uri = getUri(uriBuilder);

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);

			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return Status.valueOf(getString(response));
			} catch (InsufficientStorageException | DataNotOnlineException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	private String getString(CloseableHttpResponse response) throws InternalException,
			BadRequestException, DataNotOnlineException, ParseException,
			InsufficientPrivilegesException, NotImplementedException, InsufficientStorageException,
			NotFoundException, IOException {
		checkStatus(response);
		HttpEntity entity = response.getEntity();
		if (entity == null) {
			throw new InternalException("No http entity returned in response");
		}
		return EntityUtils.toString(entity);
	}

	private URI getUri(URIBuilder uriBuilder) throws InternalException, BadRequestException {
		try {
			URI uri = uriBuilder.build();
			if (uri.toString().length() > 2048) {
				throw new BadRequestException("Generated URI is of length "
						+ uri.toString().length() + " which exceeds 2048");
			}
			return uri;
		} catch (URISyntaxException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	private URIBuilder getUriBuilder(String path) {
		return new URIBuilder(idsUri).setPath(basePath + path);

	}

	/**
	 * Returns true if the data identified by the preparedId returned by a call to prepareData is
	 * ready.
	 * 
	 * @param preparedId
	 *            the id returned by a call to prepareData
	 * 
	 * @return true if ready otherwise false.
	 * 
	 * @throws BadRequestException
	 * @throws NotFoundException
	 * @throws InternalException
	 * @throws NotImplementedException
	 */
	public boolean isPrepared(String preparedId) throws BadRequestException, NotFoundException,
			InternalException, NotImplementedException {
		URIBuilder uriBuilder = getUriBuilder("isPrepared");
		uriBuilder.setParameter("preparedId", preparedId);
		URI uri = getUri(uriBuilder);

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);

			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return Boolean.parseBoolean(getString(response));
			} catch (InsufficientStorageException | DataNotOnlineException
					| InsufficientPrivilegesException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Returns the readOnly status of the server
	 * 
	 * @return true if readonly, else false
	 * 
	 * @throws InternalException
	 * @throws NotImplementedException
	 * @throws
	 */
	public boolean isReadOnly() throws InternalException, NotImplementedException {
		URI uri;
		try {
			uri = getUri(getUriBuilder("isReadOnly"));
		} catch (BadRequestException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return Boolean.parseBoolean(getString(response));
			} catch (IOException | InsufficientStorageException | DataNotOnlineException
					| BadRequestException | InsufficientPrivilegesException | NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Returns the twoLevel status of the server
	 * 
	 * @return true if the server uses both main and archive storage, else false
	 * 
	 * @throws InternalException
	 * @throws NotImplementedException
	 * @throws
	 */
	public boolean isTwoLevel() throws InternalException, NotImplementedException {
		URI uri;
		try {
			uri = getUri(getUriBuilder("isTwoLevel"));
		} catch (BadRequestException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return Boolean.parseBoolean(getString(response));
			} catch (IOException | InsufficientStorageException | DataNotOnlineException
					| BadRequestException | InsufficientPrivilegesException | NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Check that the server is alive and is an IDS server
	 * 
	 * @throws InternalException
	 * @throws NotFoundException
	 *             If the server gives an unexpected response
	 * @throws NotImplementedException
	 * @throws
	 */
	public void ping() throws InternalException, NotFoundException, NotImplementedException {
		URI uri;
		try {
			uri = getUri(getUriBuilder("ping"));
		} catch (BadRequestException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {

			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				String result = getString(response);
				if (!result.equals("IdsOK")) {
					throw new NotFoundException("Server gave invalid response: " + result);
				}
			} catch (IOException | InsufficientStorageException | DataNotOnlineException
					| BadRequestException | InsufficientPrivilegesException | NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Prepare data for a subsequent getData call.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
	 * @param flags
	 *            To select packing options
	 * 
	 * @return a prepareId to be used in calls to getData and getStatus
	 * 
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws NotFoundException
	 * @throws InternalException
	 */
	public String prepareData(String sessionId, DataSelection dataSelection, Flag flags)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			NotFoundException, InternalException {
		URI uri = getUri(getUriBuilder("prepareData"));
		List<NameValuePair> formparams = new ArrayList<>();
		formparams.add(new BasicNameValuePair("sessionId", sessionId));
		for (Entry<String, String> entry : dataSelection.getParameters().entrySet()) {
			formparams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
		}
		if (flags == Flag.ZIP || flags == Flag.ZIP_AND_COMPRESS) {
			formparams.add(new BasicNameValuePair("zip", "true"));
		}
		if (flags == Flag.COMPRESS || flags == Flag.ZIP_AND_COMPRESS) {
			formparams.add(new BasicNameValuePair("compress", "true"));
		}
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpEntity entity = new UrlEncodedFormEntity(formparams);
			HttpPost httpPost = new HttpPost(uri);
			httpPost.setEntity(entity);
			try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
				return getString(response);
			} catch (InsufficientStorageException | DataNotOnlineException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Put the data in the inputStream into a data file and catalogue it. The client generates a
	 * checksum which is compared to that produced by the server to detect any transmission errors.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param inputStream
	 *            the input stream providing the data to store
	 * @param name
	 *            the name to associate with the data file
	 * @param datasetId
	 *            the id of the ICAT "Dataset" which should own the data file
	 * @param datafileFormatId
	 *            the id of the ICAT "DatafileForat" to be associated with the data file
	 * @param description
	 *            Free text to associate with the data file (may be null)
	 * 
	 * @return the ICAT id of the "Datafile" object created
	 * 
	 * @throws BadRequestException
	 * @throws NotFoundException
	 * @throws InternalException
	 * @throws InsufficientPrivilegesException
	 * @throws NotImplementedException
	 * @throws DataNotOnlineException
	 * @throws InsufficientStorageException
	 */
	public Long put(String sessionId, InputStream inputStream, String name, long datasetId,
			long datafileFormatId, String description) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException,
			NotImplementedException, DataNotOnlineException, InsufficientStorageException {
		return put(sessionId, inputStream, name, datasetId, datafileFormatId, description, null,
				null, null);
	}

	/**
	 * Put the data in the inputStream into a data file and catalogue it. The client generates a
	 * checksum which is compared to that produced by the server to detect any transmission errors.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param inputStream
	 *            the input stream providing the data to store
	 * @param name
	 *            the name to associate with the data file
	 * @param datasetId
	 *            the id of the ICAT "Dataset" which should own the data file
	 * @param datafileFormatId
	 *            the id of the ICAT "DatafileForat" to be associated with the data file
	 * @param description
	 *            Free text to associate with the data file. (may be null)
	 * @param doi
	 *            The Digital Object Identifier to associate with the data file. (may be null)
	 * @param datafileCreateTime
	 *            the time to record as the creation time of the datafile. If null the current time
	 *            as known to the IDS server will be stored.
	 * @param datafileModTime
	 *            the time to record as the modification time of the datafile. If null the value of
	 *            the datafileCreateTime or the current time as known to the IDS server if that
	 *            value is also null will be stored.
	 * 
	 * @return the ICAT id of the "Datafile" object created.
	 * 
	 * @throws BadRequestException
	 * @throws NotFoundException
	 * @throws InternalException
	 * @throws InsufficientPrivilegesException
	 * @throws NotImplementedException
	 * @throws DataNotOnlineException
	 * @throws InsufficientStorageException
	 */
	public Long put(String sessionId, InputStream inputStream, String name, long datasetId,
			long datafileFormatId, String description, String doi, Date datafileCreateTime,
			Date datafileModTime) throws BadRequestException, NotFoundException, InternalException,
			InsufficientPrivilegesException, NotImplementedException, DataNotOnlineException,
			InsufficientStorageException {
		if (inputStream == null) {
			throw new BadRequestException("Input stream is null");
		}
		CRC32 crc = new CRC32();
		inputStream = new CheckedInputStream(inputStream, crc);
		URIBuilder uriBuilder = getUriBuilder("put");
		uriBuilder.setParameter("sessionId", sessionId).setParameter("name", name)
				.setParameter("datafileFormatId", Long.toString(datafileFormatId))
				.setParameter("datasetId", Long.toString(datasetId));
		if (description != null) {
			uriBuilder.setParameter("description", description);
		}
		if (doi != null) {
			uriBuilder.setParameter("doi", doi);
		}
		if (datafileCreateTime != null) {
			uriBuilder.setParameter("datafileCreateTime",
					Long.toString(datafileCreateTime.getTime()));
		}
		if (datafileModTime != null) {
			uriBuilder.setParameter("datafileModTime", Long.toString(datafileModTime.getTime()));
		}

		URI uri = getUri(uriBuilder);
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpPut httpPut = new HttpPut(uri);
		httpPut.setEntity(new InputStreamEntity(inputStream, ContentType.APPLICATION_OCTET_STREAM));

		try (CloseableHttpResponse response = httpclient.execute(httpPut)) {
			String result = getString(response);
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode rootNode = (ObjectNode) mapper.readValue(result, JsonNode.class);
			if (!rootNode.get("checksum").asText().equals(Long.toString(crc.getValue()))) {
				throw new InternalException("Error uploading - the checksum was not as expected");
			}
			return Long.parseLong(rootNode.get("id").asText());
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		} catch (NumberFormatException e) {
			throw new InternalException("Web service call did not return a valid Long value");
		}

	}

	/**
	 * Restore data specified by the dataSelection.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
	 * 
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws InternalException
	 * @throws NotFoundException
	 */
	public void restore(String sessionId, DataSelection dataSelection)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotFoundException {

		URI uri = getUri(getUriBuilder("restore"));
		List<NameValuePair> formparams = new ArrayList<>();
		formparams.add(new BasicNameValuePair("sessionId", sessionId));
		for (Entry<String, String> entry : dataSelection.getParameters().entrySet()) {
			formparams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
		}
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpEntity entity = new UrlEncodedFormEntity(formparams);
			HttpPost httpPost = new HttpPost(uri);
			httpPost.setEntity(entity);
			try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
				expectNothing(response);
			} catch (InsufficientStorageException | DataNotOnlineException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

}