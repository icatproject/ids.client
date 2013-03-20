package org.icatproject.idsclient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.icatproject.idsclient.exceptions.BadRequestException;
import org.icatproject.idsclient.exceptions.ForbiddenException;
import org.icatproject.idsclient.exceptions.IDSException;
import org.icatproject.idsclient.exceptions.InsufficientStorageException;
import org.icatproject.idsclient.exceptions.InternalServerErrorException;
import org.icatproject.idsclient.exceptions.NotFoundException;
import org.icatproject.idsclient.exceptions.NotImplementedException;

/**
 * An IDS client for an instance of an ICAT Data Service. Implements the download methods
 * prepareData, getStatus and getData. The extra methods getDatafile and getDataset are provided as
 * shortcut functions that perform the same function as prepareData but with parameters specific to
 * requesting only datafiles or datasets.
 */
public class Client {

    private URL idsUrl = null;

    /**
     * Initiates an IDS client for an instance of an ICAT Data Service specified by the idsURL.
     * 
     * @param idsUrl URL of the IDS RESTful webservice
     */
    public Client(URL idsUrl) {
        this.idsUrl = idsUrl;
    }

    /**
     * Send request to the IDS for the list of investigations, datasets or datafiles to be prepared for
     * download. 
     * 
     * @param sessionId        An ICAT session ID
     * @param investigationIds A list of investigation IDs (optional)
     * @param datasetIds       A list of dataset IDs (optional)
     * @param datafileIds      A list of datafile IDs (optional)
     * @param compress         Compress ZIP archive of files (dependent on ZIP parameter below) (optional)
     * @param zip              Request data to be packaged in a ZIP archive (optional)
     * @return                 A preparedId for the download request
     * @throws IDSException    Thrown for problems with IDS request
     * @throws IOException     Thrown for problems connecting to the IDS
     */
    public String prepareData(String sessionId, List<Long> investigationIds, List<Long> datasetIds,
            List<Long> datafileIds, Boolean compress, Boolean zip) throws IDSException, IOException {
        Map<String, String> parameters = new HashMap<String, String>();

        // create parameter list
        parameters.put("sessionId", sessionId);
        if (investigationIds != null)
            parameters.put("investigationIds", idListToString(investigationIds));
        if (datasetIds != null)
            parameters.put("datasetIds", idListToString(datasetIds));
        if (datafileIds != null)
            parameters.put("datafileIds", idListToString(datafileIds));
        if (compress != null)
            parameters.put("compress", compress.toString());
        if (zip != null)
            parameters.put("zip", zip.toString());

        Response response = HTTPConnect("POST", "prepareData", parameters);
        return response.getResponse().toString().trim();
    }

    /**
     * Shortcut function for preparedData. Only accepts list of dataset IDs.
     * 
     * @param sessionId     An ICAT session ID
     * @param datasetIds    A list of dataset IDs
     * @param compress      Compress ZIP archive of files (dependent on ZIP parameter below) (optional)
     * @param zip           Request data to be packaged in a ZIP archive (optional)
     * @return              A preparedId for the download request
     * @throws IDSException Thrown for problems with IDS request
     * @throws IOException  Thrown for problems connecting to the IDS
     */
    public String prepareDatafiles(String sessionId, List<Long> datasetIds, Boolean compress,
            Boolean zip) throws IDSException, IOException {
        return prepareData(sessionId, null, datasetIds, null, compress, zip);
    }

    /**
     * Shortcut function for preparedData. Only accepts list of datafile IDs.
     * 
     * @param sessionId     An ICAT session ID
     * @param datafileIds   A list of datafile IDs
     * @param compress      Compress ZIP archive of files (dependent on ZIP parameter below) (optional)
     * @param zip           Request data to be packaged in a ZIP archive (optional)
     * @return              A preparedId for the download request
     * @throws IDSException Thrown for problems with IDS request
     * @throws IOException  Thrown for problems connecting to the IDS
     */
    public String prepareDatasets(String sessionId, List<Long> datafileIds, Boolean compress,
            Boolean zip) throws IDSException, IOException {
        return prepareData(sessionId, null, null, datafileIds, compress, zip);

    }

    /**
     * Sends a request to get the status of a download request.
     * 
     * @param preparedId    The ID of the download request
     * @return              Current status of the download request (ONLINE, IMCOMPLETE, RESTORING, ARCHIVED)
     * @throws IDSException Thrown for problems with IDS request
     * @throws IOException  Thrown for problems connecting to the IDS
     */
    public String getStatus(String preparedId) throws IDSException, IOException {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("preparedId", preparedId);
        Response response = HTTPConnect("GET", "getStatus", parameters);
        return response.getResponse().toString().trim();
    }

    /**
     * Download the data for a download request. Returns an instance of the IDS Response class that contains
     * a OutputStream of data from the IDS and the HTTP header information.
     * 
     * @param preparedId    The ID of the download request
     * @param outname       The desired filename for the download (optional)
     * @param offset        The desired offset of the file (optional)
     * @return              An instance of the IDS Response class
     * @throws IDSException Thrown for problems with IDS request
     * @throws IOException  Thrown for problems connecting to the IDS
     */
    public Response getData(String preparedId, String outname, Long offset) throws IOException, IDSException {
        Map<String, String> parameters = new HashMap<String, String>();

        // create parameter list
        parameters.put("preparedId", preparedId);
        if (outname != null)
            parameters.put("outname", outname);
        if (offset != null)
            parameters.put("offset", offset.toString());

        return HTTPConnect("GET", "getData", parameters);
    }

    /**
     * Sends request to hint that the datafiles may be moved to storage where access may be slower
     * 
     * @param sessionId        An ICAT session ID
     * @param investigationIds A list of investigation IDs (optional)
     * @param datasetIds       A list of dataset IDs (optional)
     * @param datafileIds      A list of datafile IDs (optional)
     * @throws IDSException    Thrown for problems with IDS request
     * @throws IOException     Thrown for problems connecting to the IDS
     */
    public void archive(String sessionId, List<Long> investigationIds, List<Long> datasetIds,
            List<Long> datafileIds) throws IOException {
        Map<String, String> parameters = new HashMap<String, String>();

        // create parameter list
        parameters.put("sessionId", sessionId);
        if (investigationIds != null)
            parameters.put("investigationIds", idListToString(investigationIds));
        if (datasetIds != null)
            parameters.put("datasetIds", idListToString(datasetIds));
        if (datafileIds != null)
            parameters.put("datafileIds", idListToString(datafileIds));

        Response response = HTTPConnect("POST", "archive", parameters);
        response.getResponse().toString().trim();
    }
      
    /*
     * Create HTTP request of type defined by method to 'page' defined by relativeURL. Converts
     * parameter list into format suitable for either URL (GET,DELETE) or message body (POST).
     */
    protected Response HTTPConnect(String method, String relativeUrl, Map<String, String> parameters)
            throws IOException, IDSException {
        StringBuilder url = new StringBuilder();
        HttpURLConnection connection;

        // construct url
        url.append(idsUrl);
        url.append(relativeUrl);

        // add parameters to url for GET and DELETE requests
        if ("GET".equals(method) || "DELETE".equals(method)) {
            url.append("?");
            url.append(parametersToString(parameters));
        }

        // setup connection
        connection = (HttpURLConnection) new URL(url.toString()).openConnection();
        connection.setDoOutput(true);
        connection.setDoInput(true);
        connection.setUseCaches(false);
        connection.setAllowUserInteraction(false);
        connection.setRequestMethod(method);

        // add parameters to message body for POST requests
        if ("POST".equals(method)) {
            String messageBody = parametersToString(parameters);
            OutputStream os = null;
            try {
                os = connection.getOutputStream();
                os.write(messageBody.getBytes());
            } finally {
                if (os != null) {
                    os.close();
                }
            }
        }

        // read in response
        InputStream in = null;
        ByteArrayOutputStream os = null;
        try {
            os = new ByteArrayOutputStream();
            if (connection.getResponseCode() != 200) {
                in = connection.getErrorStream();
            } else {
                in = connection.getInputStream();
            }
            IOUtils.copy(in, os);
        } finally {
            if (in != null) {
                in.close();
            }
            if (os != null) {
                os.close();
            }
        }

        // convert response code into relevant IDSException
        switch (connection.getResponseCode()) {
            case 200:
                break;
            case 400:
                throw new BadRequestException(os.toString());
            case 403:
                throw new ForbiddenException(os.toString());
            case 404:
                throw new NotFoundException(os.toString());
            case 500:
                throw new InternalServerErrorException(os.toString());
            case 501:
                throw new NotImplementedException(os.toString());
            case 507:
                throw new InsufficientStorageException(os.toString());
            default:
                throw new IDSException("Unknown response " + connection.getResponseCode() + ": "
                        + os.toString());
        }

        connection.disconnect();

        return new Response(os, connection.getHeaderFields());
    }

    /*
     * Turn a list of key-value pairs into format suitable for HTTP GET request ie. key=value&key=value
     */
    private String parametersToString(Map<String, String> parameters)
            throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String, String>> it = parameters.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> pairs = (Map.Entry<String, String>) it.next();
            sb.append(pairs.getKey() + "=" + URLEncoder.encode(pairs.getValue(), "UTF-8"));
            if (it.hasNext()) {
                sb.append("&");
            }
            it.remove();
        }
        return sb.toString();
    }

    /*
     * Turn a list of Longs into comma separated list.
     */
    private String idListToString(List<Long> ids) {
        StringBuilder sb = new StringBuilder();
        Iterator<Long> it = ids.iterator();
        while (it.hasNext()) {
            sb.append(it.next());
            if (it.hasNext()) {
                sb.append(",");
            }
            it.remove();
        }
        return sb.toString();
    }
}
