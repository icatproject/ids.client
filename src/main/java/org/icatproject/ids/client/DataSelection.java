package org.icatproject.ids.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * To build set of data to be processed by the IDS.
 */
public class DataSelection {

	private Set<Long> datafileIds = new HashSet<>();

	private Set<Long> datasetIds = new HashSet<>();

	private Set<Long> investigationIds = new HashSet<>();

	/**
	 * Add a data file
	 * 
	 * @param datafileId
	 *            the id of the data file
	 * 
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addDatafile(long datafileId) {
		datafileIds.add(datafileId);
		return this;
	}

	/**
	 * Add data files
	 * 
	 * @param datafileIds
	 *            the list of data file id values
	 * 
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addDatafiles(List<Long> datafileIds) {
		this.datafileIds.addAll(datafileIds);
		return this;
	}

	/**
	 * Add data set
	 * 
	 * @param datasetId
	 *            the id of the data set
	 * 
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addDataset(long datasetId) {
		datasetIds.add(datasetId);
		return this;
	}

	/**
	 * Add data sets
	 * 
	 * @param datasetIds
	 *            the list of data set id values
	 * 
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addDatasets(List<Long> datasetIds) {
		this.datasetIds.addAll(datasetIds);
		return this;
	}

	/**
	 * Add investigation
	 * 
	 * @param investigationId
	 *            the id of the investigation
	 * 
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addInvestigation(long investigationId) {
		investigationIds.add(investigationId);
		return this;
	}

	/**
	 * Add investigations
	 * 
	 * @param investigationIds
	 *            the list of investigation id values
	 * 
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addInvestigations(List<Long> investigationIds) {
		this.investigationIds.addAll(investigationIds);
		return this;
	}

	public Map<String, String> getParameters() {
		Map<String, String> parameters = new HashMap<>();
		if (!investigationIds.isEmpty()) {
			parameters.put("investigationIds", setToString(investigationIds));
		}
		if (!datasetIds.isEmpty()) {
			parameters.put("datasetIds", setToString(datasetIds));
		}
		if (!datafileIds.isEmpty()) {
			parameters.put("datafileIds", setToString(datafileIds));
		}
		return parameters;
	}

	private String setToString(Set<Long> ids) {
		StringBuilder sb = new StringBuilder();
		for (long id : ids) {
			if (sb.length() != 0) {
				sb.append(',');
			}
			sb.append(Long.toString(id));
		}
		return sb.toString();
	}
}