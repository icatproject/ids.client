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
	 * @param datafileId
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addDatafile(long datafileId) {
		datafileIds.add(datafileId);
		return this;
	}

	/**
	 * @param datafileIds
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addDatafiles(List<Long> datafileIds) {
		datafileIds.addAll(datafileIds);
		return this;
	}

	/**
	 * @param datasetId
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addDataset(long datasetId) {
		datasetIds.add(datasetId);
		return this;
	}

	/**
	 * @param datasetIds
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addDatasets(List<Long> datasetIds) {
		datasetIds.addAll(datasetIds);
		return this;
	}

	/**
	 * @param investigationId
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addInvestigation(long investigationId) {
		investigationIds.add(investigationId);
		return this;
	}

	/**
	 * @param investigationIds
	 * @return itself to allow chaining of addXXX calls
	 */
	public DataSelection addInvestigations(List<Long> investigationIds) {
		investigationIds.addAll(investigationIds);
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