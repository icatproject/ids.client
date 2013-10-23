package org.icatproject.ids.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Client to communicate with IDS server
 */
public class IdsClient {

	public enum Flag {
		COMPRESS, NONE, ZIP, ZIP_AND_COMPRESS
	}

	private enum Method {
		DELETE, GET, POST, PUT
	}

	private enum ParmPos {
		BODY, URL
	};

	public enum Status {
		ARCHIVED, INCOMPLETE, ONLINE, RESTORING
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
	 * Archive specified data
	 * 
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
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
			process("archive", parameters, Method.POST, ParmPos.BODY, null);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}
	}

	/**
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws InternalException
	 * @throws NotFoundException
	 */
	public void delete(String sessionId, DataSelection dataSelection)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotFoundException {

		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(dataSelection.getParameters());

		try {
			process("delete", parameters, Method.DELETE, ParmPos.URL, null);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

	}

	/**
	 * @param sessionId
	 *            A valid ICAT session ID
	 * @param dataSelection
	 *            A data selection object that must not be empty
	 * @param flags
	 * @param outname
	 * @param offset
	 * @return an InputStream to allow the data to be read
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws NotFoundException
	 * @throws InternalException
	 * @throws DataNotOnlineException
	 */
	public InputStream getData(String sessionId, DataSelection data, Flag flags, String outname,
			long offset) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, NotFoundException, InternalException,
			DataNotOnlineException {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(data.getParameters());
		if (flags == Flag.ZIP || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("zip", "true");
		}
		if (flags == Flag.COMPRESS || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("compress", "true");
		}
		if (outname != null) {
			parameters.put("outname", outname);
		}
		if (offset != 0) {
			parameters.put("offset", Long.toString(offset));
		}
		HttpURLConnection urlc;
		try {
			urlc = process("getData", parameters, Method.GET, ParmPos.URL, null);
		} catch (InsufficientStorageException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

		return getStream(urlc);

	}

	/**
	 * @param preparedId
	 * @param outname
	 * @param offset
	 * @return
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
		if (offset != 0) {
			parameters.put("offset", Long.toString(offset));
		}
		HttpURLConnection urlc;
		try {
			urlc = process("getData", parameters, Method.GET, ParmPos.URL, null);
		} catch (InsufficientStorageException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

		return getStream(urlc);
	}

	/**
	 * @param preparedId
	 * @return
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws NotFoundException
	 * @throws InternalException
	 */
	public Status getStatus(String preparedId) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, NotFoundException, InternalException {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("preparedId", preparedId);

		HttpURLConnection urlc;
		try {
			urlc = process("getStatus", parameters, Method.GET, ParmPos.URL, null);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}

		return Status.valueOf(getOutput(urlc));
	}

	/**
	 * @param sessionId
	 * @param data
	 * @return
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws NotFoundException
	 * @throws InternalException
	 */
	public Status getStatus(String sessionId, DataSelection data) throws NotImplementedException,
			BadRequestException, InsufficientPrivilegesException, NotFoundException,
			InternalException {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(data.getParameters());

		HttpURLConnection urlc;

		try {
			urlc = process("getStatus", parameters, Method.GET, ParmPos.URL, null);
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
	 * @param sessionId
	 * @param data
	 * @param flags
	 * @return
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws NotFoundException
	 * @throws InternalException
	 */
	public String prepareData(String sessionId, DataSelection data, Flag flags)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			NotFoundException, InternalException {
		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(data.getParameters());
		if (flags == Flag.ZIP || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("zip", "true");
		}
		if (flags == Flag.COMPRESS || flags == Flag.ZIP_AND_COMPRESS) {
			parameters.put("compress", "true");
		}
		HttpURLConnection urlc;
		try {
			urlc = process("prepareData", parameters, Method.POST, ParmPos.BODY, null);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}
		return getOutput(urlc);
	}

	/**
	 * @param relativeUrl
	 * @param parameters
	 * @param method
	 * @param parmPos
	 * @param inputStream
	 * @return
	 * @throws InternalException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws InsufficientStorageException
	 * @throws NotFoundException
	 * @throws NotImplementedException
	 * @throws DataNotOnlineException
	 */
	public HttpURLConnection process(String relativeUrl, Map<String, String> parameters,
			Method method, ParmPos parmPos, InputStream inputStream) throws InternalException,
			BadRequestException, InsufficientPrivilegesException, InsufficientStorageException,
			NotFoundException, NotImplementedException, DataNotOnlineException {
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
				OutputStream os = null;
				try {
					os = urlc.getOutputStream();
					os.write(parms.getBytes());
				} finally {
					if (os != null) {
						os.close();
					}
				}

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
	 * @param sessionId
	 * @param file
	 * @param name
	 * @param datasetId
	 * @param datafileFormatId
	 * @param description
	 * @return
	 * @throws BadRequestException
	 * @throws NotFoundException
	 * @throws InternalException
	 * @throws InsufficientPrivilegesException
	 * @throws NotImplementedException
	 * @throws DataNotOnlineException
	 * @throws InsufficientStorageException
	 */
	public Long put(String sessionId, File file, String name, long datasetId,
			long datafileFormatId, String description) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException,
			NotImplementedException, DataNotOnlineException, InsufficientStorageException {
		return put(sessionId, file, name, datasetId, datafileFormatId, description, null, null,
				null);
	}

	/**
	 * @param sessionId
	 * @param file
	 * @param name
	 * @param datasetId
	 * @param datafileFormatId
	 * @param description
	 * @param doi
	 * @param datafileCreateTime
	 * @param datafileModTime
	 * @return
	 * @throws BadRequestException
	 * @throws NotFoundException
	 * @throws InternalException
	 * @throws InsufficientPrivilegesException
	 * @throws NotImplementedException
	 * @throws DataNotOnlineException
	 * @throws InsufficientStorageException
	 */
	/**
	 * @param sessionId
	 * @param file
	 * @param name
	 * @param datasetId
	 * @param datafileFormatId
	 * @param description
	 * @param doi
	 * @param datafileCreateTime
	 * @param datafileModTime
	 * @return
	 * @throws BadRequestException
	 * @throws NotFoundException
	 * @throws InternalException
	 * @throws InsufficientPrivilegesException
	 * @throws NotImplementedException
	 * @throws DataNotOnlineException
	 * @throws InsufficientStorageException
	 */
	public Long put(String sessionId, File file, String name, long datasetId,
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

		try {
			HttpURLConnection urlc = process("put", parameters, Method.PUT, ParmPos.URL,
					new FileInputStream(file));
			return Long.parseLong(getOutput(urlc));
		} catch (FileNotFoundException e) {
			throw new NotFoundException("File " + file.getAbsolutePath() + " does not exist");
		} catch (NumberFormatException e) {
			throw new InternalException("Web service call did not return a valid Long value");
		}

	}

	/**
	 * @param sessionId
	 * @param data
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws InsufficientPrivilegesException
	 * @throws InternalException
	 * @throws NotFoundException
	 */
	public void restore(String sessionId, DataSelection data) throws NotImplementedException,
			BadRequestException, InsufficientPrivilegesException, InternalException,
			NotFoundException {

		Map<String, String> parameters = new HashMap<>();
		parameters.put("sessionId", sessionId);
		parameters.putAll(data.getParameters());

		try {
			process("restore", parameters, Method.POST, ParmPos.BODY, null);
		} catch (InsufficientStorageException | DataNotOnlineException e) {
			throw new InternalException("Unexpected exception " + e.getClass() + " "
					+ e.getMessage());
		}
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
			urlc = process("ping", emptyMap, Method.GET, ParmPos.URL, null);
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

}