package org.icatproject.ids.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

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

	private enum Method {
		DELETE, GET, POST, PUT
	}

	private enum ParmPos {
		BODY, URL
	};

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

	private static String getOutput(HttpURLConnection urlc) throws InternalException {
		try {
			InputStream stream = urlc.getInputStream();
			ByteArrayOutputStream os = null;
			try {
				os = new ByteArrayOutputStream();
				int len;
				byte[] buffer = new byte[1024];
				while ((len = stream.read(buffer)) != -1) {
					os.write(buffer, 0, len);
				}
				return os.toString().trim();
			} finally {
				if (stream != null) {
					stream.close();
				}
			}
		} catch (IOException e) {
			throw new InternalException("IOException " + e.getMessage());
		}
	}

	private final int BUFSIZ = 2048;

	private URL idsUrl;

	/**
	 * @param idsUrl
	 *            The URL of the ids server host. This should be of the form
	 *            https://example.com:443.
	 */
	public IdsClient(URL idsUrl) {
		String file = idsUrl.getFile();
		if (!file.endsWith("/")) {
			file = file + "/";
		}
		file = file + "ids/";
		try {
			this.idsUrl = new URL(idsUrl.getProtocol(), idsUrl.getHost(), idsUrl.getPort(), file);
		} catch (MalformedURLException e) {
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

		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(dataSelection.getParameters());

		try {
			process("archive", parameters, Method.POST, ParmPos.BODY, null, null);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
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

		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(dataSelection.getParameters());

		try {
			process("delete", parameters, Method.DELETE, ParmPos.URL, null, null);
		} catch (InsufficientStorageException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
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
	 * @return an InputStream to allow the data to be read
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
		HttpURLConnection urlc;
		Map<String, String> headers = null;
		if (offset != 0) {
			headers = new HashMap<>();
			headers.put("Range", "bytes=" + offset + "-");
		}
		try {
			urlc = process("getData", parameters, Method.GET, ParmPos.URL, headers, null);
		} catch (InsufficientStorageException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

		return getStream(urlc);

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
	 * @return an InputStream to allow the data to be read
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
		Map<String, String> parameters = new HashMap<>();
		parameters.put("preparedId", preparedId);
		if (outname != null) {
			parameters.put("outname", outname);
		}
		HttpURLConnection urlc;
		Map<String, String> headers = null;
		if (offset != 0) {
			headers = new HashMap<>();
			headers.put("Range", "bytes=" + offset + "-");
		}
		try {
			urlc = process("getData", parameters, Method.GET, ParmPos.URL, headers, null);
		} catch (InsufficientStorageException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

		return getStream(urlc);
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
			return new URL(url + "?" + sb.toString());
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
	 */
	public ServiceStatus getServiceStatus(String sessionId) throws InternalException,
			InsufficientPrivilegesException {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		HttpURLConnection urlc;
		try {
			urlc = process("getServiceStatus", parameters, Method.GET, ParmPos.URL, null, null);
		} catch (InsufficientStorageException | DataNotOnlineException | InternalException
				| BadRequestException | NotFoundException | NotImplementedException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}
		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode rootNode = mapper.readValue(urlc.getInputStream(), JsonNode.class);
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
		} catch (IOException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
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
		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(dataSelection.getParameters());

		HttpURLConnection urlc;

		try {
			urlc = process("getStatus", parameters, Method.GET, ParmPos.URL, null, null);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

		return Status.valueOf(getOutput(urlc));
	}

	private InputStream getStream(HttpURLConnection urlc) throws InternalException {
		try {
			return urlc.getInputStream();
		} catch (IOException e) {
			throw new InternalException("IOException " + e.getMessage());
		}
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
		Map<String, String> parameters = new HashMap<>();
		parameters.put("preparedId", preparedId);

		HttpURLConnection urlc;
		try {
			urlc = process("isPrepared", parameters, Method.GET, ParmPos.URL, null, null);
		} catch (InsufficientStorageException | DataNotOnlineException
				| InsufficientPrivilegesException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}
		return Boolean.parseBoolean(getOutput(urlc));
	}

	/**
	 * Check that the server is alive and is an IDS server
	 * 
	 * @throws InternalException
	 * @throws NotFoundException
	 *             If the server gives an unexpected response
	 */
	public void ping() throws InternalException, NotFoundException {
		Map<String, String> emptyMap = Collections.emptyMap();
		HttpURLConnection urlc;
		try {
			urlc = process("ping", emptyMap, Method.GET, ParmPos.URL, null, null);
		} catch (InsufficientStorageException | DataNotOnlineException | InternalException
				| BadRequestException | InsufficientPrivilegesException | NotFoundException
				| NotImplementedException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}
		String result = getOutput(urlc);
		if (!result.equals("IdsOK")) {
			throw new NotFoundException("Server gave invalid response: " + result);
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
		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(dataSelection.getParameters());
		if (flags == Flag.ZIP || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("zip", "true");
		}
		if (flags == Flag.COMPRESS || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("compress", "true");
		}
		HttpURLConnection urlc;
		try {
			urlc = process("prepareData", parameters, Method.POST, ParmPos.BODY, null, null);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}
		return getOutput(urlc);
	}

	private HttpURLConnection process(String relativeUrl, Map<String, String> parameters,
			Method method, ParmPos parmPos, Map<String, String> headers, InputStream inputStream)
			throws InternalException, BadRequestException, InsufficientPrivilegesException,
			InsufficientStorageException, NotFoundException, NotImplementedException,
			DataNotOnlineException {
		HttpURLConnection urlc;
		int rc;
		try {
			URL url;
			url = new URL(idsUrl, relativeUrl);

			String parms = null;

			if (!parameters.isEmpty()) {

				StringBuilder sb = new StringBuilder();
				for (Entry<String, String> e : parameters.entrySet()) {
					if (sb.length() != 0) {
						sb.append("&");
					}
					sb.append(e.getKey() + "=" + URLEncoder.encode(e.getValue(), "UTF-8"));
				}
				parms = sb.toString();
			}

			if (parmPos == ParmPos.URL && parms != null) {
				url = new URL(url + "?" + parms);
			}

			urlc = (HttpURLConnection) url.openConnection();
			if (!parameters.isEmpty()) {
				urlc.setDoOutput(true);
			}

			urlc.setUseCaches(false);
			urlc.setRequestMethod(method.name());

			if (headers != null) {
				for (Entry<String, String> entry : headers.entrySet()) {
					urlc.setRequestProperty(entry.getKey(), entry.getValue());
				}
			}

			if (parmPos == ParmPos.BODY && parms != null) {

				OutputStream os = null;
				try {
					os = urlc.getOutputStream();
					os.write(parms.getBytes());
				} finally {
					if (os != null) {
						os.close();
					}
				}
			}

			if (inputStream != null) {
				urlc.setChunkedStreamingMode(8192);
				BufferedOutputStream bos = null;
				BufferedInputStream bis = null;
				try {
					int bytesRead = 0;
					byte[] buffer = new byte[BUFSIZ];
					bis = new BufferedInputStream(inputStream);
					bos = new BufferedOutputStream(urlc.getOutputStream());

					// write bytes to output stream
					while ((bytesRead = bis.read(buffer)) > 0) {
						bos.write(buffer, 0, bytesRead);
					}
				} finally {
					if (bis != null) {
						bis.close();
					}
					if (bos != null) {
						bos.close();
					}
				}
			}

			rc = urlc.getResponseCode();
		} catch (Exception e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		if (rc / 100 != 2) {
			String error = null;
			String code;
			String message;
			try {
				InputStream stream = urlc.getErrorStream();
				ByteArrayOutputStream os = null;
				try {
					os = new ByteArrayOutputStream();
					int len;
					byte[] buffer = new byte[1024];
					while ((len = stream.read(buffer)) != -1) {
						os.write(buffer, 0, len);
					}
					error = os.toString();
				} finally {
					if (stream != null) {
						stream.close();
					}
				}
				ObjectMapper om = new ObjectMapper();
				JsonNode rootNode = om.readValue(error, JsonNode.class);
				code = rootNode.get("code").asText();
				message = rootNode.get("message").asText();
			} catch (Exception e) {
				throw new InternalException("TestingClient " + error);
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
		return urlc;
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
		Map<String, String> parameters = new HashMap<>();

		parameters.put("sessionId", sessionId);
		parameters.put("name", name);
		parameters.put("datafileFormatId", Long.toString(datafileFormatId));
		parameters.put("datasetId", Long.toString(datasetId));
		if (description != null) {
			parameters.put("description", description);
		}
		if (doi != null) {
			parameters.put("doi", doi);
		}
		if (datafileCreateTime != null) {
			parameters.put("datafileCreateTime", Long.toString(datafileCreateTime.getTime()));
		}
		if (datafileModTime != null) {
			parameters.put("datafileModTime", Long.toString(datafileModTime.getTime()));
		}

		if (inputStream == null) {
			throw new BadRequestException("Input stream is null");
		}
		CRC32 crc = new CRC32();
		inputStream = new CheckedInputStream(inputStream, crc);
		HttpURLConnection urlc = process("put", parameters, Method.PUT, ParmPos.URL, null,
				inputStream);
		ObjectMapper mapper = new ObjectMapper();

		try {
			ObjectNode rootNode = (ObjectNode) mapper.readValue(urlc.getInputStream(),
					JsonNode.class);
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

		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(dataSelection.getParameters());

		try {
			process("restore", parameters, Method.POST, ParmPos.BODY, null, null);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}
	}

}