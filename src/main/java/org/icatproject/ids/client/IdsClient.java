package org.icatproject.ids.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import javax.json.Json;
import javax.json.JsonException;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
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

		private int lockCount;
		private Set<Long> lockedDs = new HashSet<>();
		private Map<String, String> opItems = new HashMap<>();

		/**
		 * Return the number of locks on groups of datasets.
		 * 
		 * @return the number of locks
		 */
		public int getLockCount() {
			return lockCount;
		}

		/**
		 * Return the ids of the locked datasets. A dataset may be included in
		 * more than one lock.
		 * 
		 * @return the ids of the locked datasets.
		 */
		public Set<Long> getLockedDs() {
			return lockedDs;
		}

		/**
		 * Return a map from a description of a data set to the requested state.
		 * 
		 * @return map
		 */
		public Map<String, String> getOpItems() {
			return opItems;
		}

		void setLockedCount(int lockCount) {
			this.lockCount = lockCount;
		}

		void storeLockedDs(Long dsId) {
			lockedDs.add(dsId);
		}

		void storeOpItems(String dsInfo, String request) {
			opItems.put(dsInfo, request);
		}

	};

	/**
	 * Values to be returned by the {@code getStatus()} calls.
	 */
	public enum Status {
		/**
		 * When some or all of the requested data are not on line and
		 * restoration has not been requested
		 */
		ARCHIVED,

		/**
		 * All requested data are on line.
		 */

		ONLINE,

		/**
		 * When some or all of the requested data are not on line but otherwise
		 * restoration has been requested.
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
	 *             if the operation has not been implemented.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws NotFoundException
	 *             if some part of the data is not known to ICAT.
	 */
	public void archive(String sessionId, DataSelection dataSelection) throws NotImplementedException,
			BadRequestException, InsufficientPrivilegesException, InternalException, NotFoundException {

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
			DataNotOnlineException, IOException, InsufficientPrivilegesException, NotImplementedException,
			InsufficientStorageException, NotFoundException {
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
			try (JsonReader jsonReader = Json.createReader(new StringReader(error))) {
				JsonObject json = jsonReader.readObject();
				code = json.getString("code");
				message = json.getString("message");
			} catch (JsonException e) {
				throw new InternalException("Status code " + rc + " returned but message not json: " + error);
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
	 *             if the operation has not been implemented.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws NotFoundException
	 *             if some part of the data is not known to ICAT
	 * @throws DataNotOnlineException
	 *             if some of the data are not online.
	 */
	public void delete(String sessionId, DataSelection dataSelection) throws NotImplementedException,
			BadRequestException, InsufficientPrivilegesException, InternalException, NotFoundException,
			DataNotOnlineException {

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

	private void expectNothing(CloseableHttpResponse response) throws InternalException, BadRequestException,
			DataNotOnlineException, InsufficientPrivilegesException, NotImplementedException,
			InsufficientStorageException, NotFoundException, IOException {
		checkStatus(response);
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			if (!EntityUtils.toString(entity).isEmpty()) {
				throw new InternalException("No http entity expected in response");
			}
		}
	}

	/**
	 * Get the version of the IDS server
	 * 
	 * @return a String with the version of the IDS server
	 * 
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 */
	public String getApiVersion() throws InternalException, NotImplementedException {
		URI uri;
		try {
			uri = getUri(getUriBuilder("getApiVersion"));
		} catch (BadRequestException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return getString(response);
			} catch (IOException | InsufficientStorageException | DataNotOnlineException | BadRequestException
					| InsufficientPrivilegesException | NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Get the data specified by the dataSelection.
	 * 
	 * @deprecated As of release 1.3, replaced by call without outname}
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
	 * @param flags
	 *            To select packing options
	 * @param outname
	 *            The name of the file. If it is in .zip format the .zip
	 *            extension will be added if not present.
	 * @param offset
	 *            Skip this number of bytes in the returned stream
	 * 
	 * @return an InputStream to allow the data to be read. Please remember to
	 *         close the stream when you have finished with it.
	 * 
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws NotFoundException
	 *             if some part of the data is not known to ICAT
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws DataNotOnlineException
	 *             if some of the data are not online.
	 */
	@Deprecated
	public InputStream getData(String sessionId, DataSelection dataSelection, Flag flags, String outname, long offset)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, NotFoundException,
			InternalException, DataNotOnlineException {
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
	 * Get the data specified by the dataSelection.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
	 * @param flags
	 *            To select packing options
	 * @param offset
	 *            Skip this number of bytes in the returned stream
	 * 
	 * @return an InputStream to allow the data to be read. Please remember to
	 *         close the stream when you have finished with it.
	 * 
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws NotFoundException
	 *             if some part of the data is not known to ICAT.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws DataNotOnlineException
	 *             if some of the data are not online.
	 */
	public InputStream getData(String sessionId, DataSelection dataSelection, Flag flags, long offset)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, NotFoundException,
			InternalException, DataNotOnlineException {
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
	 * @deprecated As of release 1.3, replaced by call without outname}
	 * 
	 * @param preparedId
	 *            A valid preparedId returned by a call to prepareData
	 * 
	 * @param outname
	 *            The name of the file. If it is in .zip format the .zip
	 *            extension will be added if not present.
	 * @param offset
	 *            Skip this number of bytes in the returned stream
	 * 
	 * @return an InputStream to allow the data to be read. Please remember to
	 *         close the stream when you have finished with it.
	 * 
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws NotFoundException
	 *             if the preparedId is not recognised.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws DataNotOnlineException
	 *             if some of the data are not online.
	 */
	@Deprecated
	public InputStream getData(String preparedId, String outname, long offset) throws NotImplementedException,
			BadRequestException, InsufficientPrivilegesException, NotFoundException, InternalException,
			DataNotOnlineException {
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

	/**
	 * Get the data using the preparedId returned by a call to prepareData
	 * 
	 * @param preparedId
	 *            A valid preparedId returned by a call to prepareData
	 * 
	 * @param offset
	 *            Skip this number of bytes in the returned stream
	 * 
	 * @return an InputStream to allow the data to be read. Please remember to
	 *         close the stream when you have finished with it.
	 * 
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws NotFoundException
	 *             if the preparedId is not recognised.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws DataNotOnlineException
	 *             if some of the data are not online.
	 */
	public InputStream getData(String preparedId, long offset) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, NotFoundException, InternalException, DataNotOnlineException {
		URIBuilder uriBuilder = getUriBuilder("getData");
		uriBuilder.setParameter("preparedId", preparedId);

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
				throw new BadRequestException("Generated URL is of length " + url.toString().length()
						+ " which exceeds 2048");
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
	 *            The name of the file. If it is in .zip format the .zip
	 *            extension will be added if not present.
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
	 * Get the URL to retrieve the data specified by the preparedId returned by
	 * a call to prepareData
	 * 
	 * @param preparedId
	 *            A valid preparedId returned by a call to prepareData
	 * 
	 * @param outname
	 *            The name of the file. If it is in .zip format the .zip
	 *            extension will be added if not present.
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
	 * Return the URL of the ICAT to which the IDS authorizes operations on the
	 * basis of a sessionId
	 * 
	 * @return the requested URL
	 * 
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 */
	public URL getIcatUrl() throws InternalException, NotImplementedException, BadRequestException {
		URI uri = getUri(getUriBuilder("getIcatUrl"));
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);
			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return new URL(getString(response));
			} catch (IOException | InsufficientStorageException | DataNotOnlineException | BadRequestException
					| InsufficientPrivilegesException | NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Return a hard link to a data file.
	 * 
	 * This is only useful in those cases where the user has direct access to
	 * the file system where the IDS is storing data. The caller is only granted
	 * read access to the file.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param datafileId
	 *            the id of a data file
	 * 
	 * @return the path of the created link.
	 * 
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws NotFoundException
	 *             if the data file is not known to ICAT.
	 * @throws DataNotOnlineException
	 *             if the data file is not online.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented. if the user does
	 *             not have direct access to the file system where the IDS is
	 *             storing data
	 */
	public Path getLink(String sessionId, long datafileId) throws BadRequestException, InsufficientPrivilegesException,
			InternalException, NotFoundException, DataNotOnlineException, NotImplementedException {
		URI uri = getUri(getUriBuilder("getLink"));
		List<NameValuePair> formparams = new ArrayList<>();
		formparams.add(new BasicNameValuePair("sessionId", sessionId));
		formparams.add(new BasicNameValuePair("datafileId", Long.toString(datafileId)));
		formparams.add(new BasicNameValuePair("username", System.getProperty("user.name")));

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpPost httpPost = new HttpPost(uri);
			httpPost.setEntity(new UrlEncodedFormEntity(formparams));
			try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
				return Paths.get(getString(response));
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
	 * To use this call, the user represented by the sessionId must be in the
	 * set of rootUserNames defined in the IDS configuration.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID of a user in the IDS rootUserNames
	 *            set.
	 * 
	 * @return a ServiceStatus object
	 * 
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 */
	public ServiceStatus getServiceStatus(String sessionId) throws InternalException, InsufficientPrivilegesException,
			NotImplementedException {
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
				try (JsonReader jsonReader = Json.createReader(new StringReader(result))) {
					ServiceStatus serviceStatus = new ServiceStatus();
					JsonObject rootNode = jsonReader.readObject();
					for (JsonValue on : rootNode.getJsonArray("opsQueue")) {
						String dsInfo = ((JsonObject) on).getString("data");
						String request = ((JsonObject) on).getString("request");
						serviceStatus.storeOpItems(dsInfo, request);
					}
					serviceStatus.setLockedCount(rootNode.getInt("lockCount"));
					for (JsonValue num : rootNode.getJsonArray("lockedIds")) {
						Long dsId = ((JsonNumber) num).longValueExact();
						serviceStatus.storeLockedDs(dsId);
					}
					return serviceStatus;
				} catch (JsonException e) {
					throw new InternalException(e.getClass() + " " + e.getMessage() + " from " + result);
				}

			} catch (InsufficientStorageException | DataNotOnlineException | InternalException | BadRequestException
					| NotFoundException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Returns size of the datafiles described by the dataSelection. This is not
	 * the same as the size of a zip file containing these datafiles.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object
	 * 
	 * @return the total size in bytes
	 * 
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws NotFoundException
	 *             if some part of the data is not known to ICAT.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 */
	public long getSize(String sessionId, DataSelection dataSelection) throws BadRequestException, NotFoundException,
			InsufficientPrivilegesException, InternalException, NotImplementedException {

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
			} catch (IOException | InsufficientStorageException | DataNotOnlineException | NumberFormatException e) {
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
	 *             if the operation has not been implemented.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws NotFoundException
	 *             if some part of the data is not known to ICAT.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 */
	public Status getStatus(String sessionId, DataSelection dataSelection) throws BadRequestException,
			NotFoundException, InsufficientPrivilegesException, InternalException, NotImplementedException {
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

	private String getString(CloseableHttpResponse response) throws InternalException, BadRequestException,
			DataNotOnlineException, InsufficientPrivilegesException, NotImplementedException,
			InsufficientStorageException, NotFoundException, IOException {
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
				throw new BadRequestException("Generated URI is of length " + uri.toString().length()
						+ " which exceeds 2048");
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
	 * Returns true if the data identified by the preparedId returned by a call
	 * to prepareData is ready.
	 * 
	 * @param preparedId
	 *            the id returned by a call to prepareData
	 * 
	 * @return true if ready otherwise false.
	 * 
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws NotFoundException
	 *             if the preparedId is not known to ICAT.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 */
	public boolean isPrepared(String preparedId) throws BadRequestException, NotFoundException, InternalException,
			NotImplementedException {
		URIBuilder uriBuilder = getUriBuilder("isPrepared");
		uriBuilder.setParameter("preparedId", preparedId);
		URI uri = getUri(uriBuilder);

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);

			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				return Boolean.parseBoolean(getString(response));
			} catch (InsufficientStorageException | DataNotOnlineException | InsufficientPrivilegesException e) {
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
	 *             if some unexpected problem should occur.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
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
			} catch (InsufficientStorageException | DataNotOnlineException | BadRequestException
					| InsufficientPrivilegesException | NotFoundException e) {
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
	 *             if some unexpected problem should occur.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
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
			} catch (IOException | InsufficientStorageException | DataNotOnlineException | BadRequestException
					| InsufficientPrivilegesException | NotFoundException e) {
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
	 *             if some unexpected problem should occur.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 */
	public void ping() throws InternalException, NotImplementedException {
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
					throw new InternalException("Server gave invalid response: " + result);
				}
			} catch (InsufficientStorageException | DataNotOnlineException | BadRequestException
					| InsufficientPrivilegesException | NotFoundException e) {
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
	 *             if the operation has not been implemented.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws NotFoundException
	 *             if a part of the data is not known to ICAT.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 */
	public String prepareData(String sessionId, DataSelection dataSelection, Flag flags)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, NotFoundException,
			InternalException {
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
	 * Put the data in the inputStream into a data file and catalogue it. The
	 * client generates a checksum which is compared to that produced by the
	 * server to detect any transmission errors.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param inputStream
	 *            the input stream providing the data to store
	 * @param name
	 *            the name to associate with the data file
	 * @param datasetId
	 *            the id of the ICAT data set which should own the data file
	 * @param datafileFormatId
	 *            the id of the ICAT "DatafileForat" to be associated with the
	 *            data file
	 * @param description
	 *            Free text to associate with the data file (may be null)
	 * 
	 * @return the ICAT id of the data file object created
	 * 
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws NotFoundException
	 *             if the data set with an id of datasetId is not known to ICAT.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 * @throws DataNotOnlineException
	 *             if the data set, datasetId, is not online.
	 * @throws InsufficientStorageException
	 *             if there is insufficient storage space to store the file.
	 */
	public Long put(String sessionId, InputStream inputStream, String name, long datasetId, long datafileFormatId,
			String description) throws BadRequestException, NotFoundException, InternalException,
			InsufficientPrivilegesException, NotImplementedException, DataNotOnlineException,
			InsufficientStorageException {
		return put(sessionId, inputStream, name, datasetId, datafileFormatId, description, null, null, null);
	}

	/**
	 * Put the data in the inputStream into a data file and catalogue it. The
	 * client generates a checksum which is compared to that produced by the
	 * server to detect any transmission errors.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param inputStream
	 *            the input stream providing the data to store
	 * @param name
	 *            the name to associate with the data file
	 * @param datasetId
	 *            the id of the ICAT data set which should own the data file
	 * @param datafileFormatId
	 *            the id of the ICAT "DatafileFormat" to be associated with the
	 *            data file
	 * @param description
	 *            Free text to associate with the data file. (may be null)
	 * @param doi
	 *            The Digital Object Identifier to associate with the data file.
	 *            (may be null)
	 * @param datafileCreateTime
	 *            the time to record as the creation time of the data file. If
	 *            null the current time as known to the IDS server will be
	 *            stored.
	 * @param datafileModTime
	 *            the time to record as the modification time of the data file.
	 *            If null the value of the datafileCreateTime or the current
	 *            time as known to the IDS server if that value is also null
	 *            will be stored.
	 * 
	 * @return the ICAT id of the data file object created.
	 * 
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws NotFoundException
	 *             if the data set with id datasetId is not known to ICAT.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 * @throws DataNotOnlineException
	 *             if the data set, datasetId, is not online.
	 * @throws InsufficientStorageException
	 *             if there is insufficient storage space to store the file.
	 */
	public Long put(String sessionId, InputStream inputStream, String name, long datasetId, long datafileFormatId,
			String description, String doi, Date datafileCreateTime, Date datafileModTime) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException, NotImplementedException,
			DataNotOnlineException, InsufficientStorageException {
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
			uriBuilder.setParameter("datafileCreateTime", Long.toString(datafileCreateTime.getTime()));
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
			try (JsonReader jsonReader = Json.createReader(new StringReader(result))) {
				JsonObject rootNode = jsonReader.readObject();
				if (rootNode.getJsonNumber("checksum").longValueExact() != crc.getValue()) {
					throw new InternalException("Error uploading - the checksum was not as expected");
				}
				return rootNode.getJsonNumber("id").longValueExact();
			}
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
	 *             if the operation has not been implemented.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws NotFoundException
	 *             if some part of the data is not known to ICAT.
	 */
	public void restore(String sessionId, DataSelection dataSelection) throws NotImplementedException,
			BadRequestException, InsufficientPrivilegesException, InternalException, NotFoundException {

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

	/**
	 * Return list of id values of data files included in the preparedId
	 * returned by a call to prepareData
	 * 
	 * @param preparedId
	 *            A valid preparedId returned by a call to prepareData
	 * 
	 * @return a list of id values
	 * 
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws NotFoundException
	 *             if the preparedId is not known to ICAT.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 */
	public List<Long> getDatafileIds(String preparedId) throws InternalException, BadRequestException,
			NotFoundException, NotImplementedException {

		URIBuilder uriBuilder = getUriBuilder("getDatafileIds");
		uriBuilder.setParameter("preparedId", preparedId);
		URI uri = getUri(uriBuilder);

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);

			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				String result = getString(response);
				try (JsonReader jsonReader = Json.createReader(new StringReader(result))) {

					JsonObject rootNode = jsonReader.readObject();
					List<Long> ids = new ArrayList<>();
					for (JsonValue num : rootNode.getJsonArray("ids")) {
						Long id = ((JsonNumber) num).longValueExact();
						ids.add(id);
					}
					return ids;
				} catch (JsonException e) {
					throw new InternalException(e.getClass() + " " + e.getMessage() + " from " + result);
				}

			} catch (InsufficientStorageException | DataNotOnlineException | InsufficientPrivilegesException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	/**
	 * Return list of id values of data files specified by the dataSelection.
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param data
	 *            A data selection object
	 * 
	 * @return a list of id values
	 * 
	 * @throws InternalException
	 *             if some unexpected problem should occur.
	 * @throws BadRequestException
	 *             if an argument is invalid.
	 * @throws NotFoundException
	 *             if some part of the data is not known to ICAT.
	 * @throws NotImplementedException
	 *             if the operation has not been implemented.
	 * @throws InsufficientPrivilegesException
	 *             if your are not authorized to perform the operation.
	 */
	public List<Long> getDatafileIds(String sessionId, DataSelection data) throws InternalException,
			BadRequestException, NotFoundException, NotImplementedException, InsufficientPrivilegesException {
		URIBuilder uriBuilder = getUriBuilder("getDatafileIds");
		uriBuilder.setParameter("sessionId", sessionId);
		for (Entry<String, String> entry : data.getParameters().entrySet()) {
			uriBuilder.setParameter(entry.getKey(), entry.getValue());
		}
		URI uri = getUri(uriBuilder);

		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(uri);

			try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
				String result = getString(response);
				try (JsonReader jsonReader = Json.createReader(new StringReader(result))) {

					JsonObject rootNode = jsonReader.readObject();
					List<Long> ids = new ArrayList<>();
					for (JsonValue num : rootNode.getJsonArray("ids")) {
						Long id = ((JsonNumber) num).longValueExact();
						ids.add(id);
					}
					return ids;
				} catch (JsonException e) {
					throw new InternalException(e.getClass() + " " + e.getMessage() + " from " + result);
				}

			} catch (InsufficientStorageException | DataNotOnlineException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

}