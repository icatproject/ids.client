package org.icatproject.ids.client;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class DataSelectionTest {

	@Test
	public void addDatafileTest() {
		DataSelection dataSelection = new DataSelection();
		Long id1 = new Long(1);
		Long id2 = new Long(2);

		dataSelection.addDatafile(id1);
		dataSelection.addDatafile(id2);

		Map<String, String> map = dataSelection.getParameters();
		String mapIds = map.get("datafileIds");

		assertEquals("1,2", mapIds);
	}

	@Test
	public void addDatafilesTest() {
		DataSelection dataSelection = new DataSelection();

		List<Long> ids = new ArrayList<Long>();
		Long id1 = new Long(1);
		Long id2 = new Long(2);
		ids.add(id1);
		ids.add(id2);

		dataSelection.addDatafiles(ids);

		Map<String, String> map = dataSelection.getParameters();
		String mapIds = map.get("datafileIds");

		assertEquals("1,2", mapIds);
	}

	@Test
	public void addDataSetTest() {
		DataSelection dataSelection = new DataSelection();

		Long id1 = new Long(1);
		Long id2 = new Long(2);

		dataSelection.addDataset(id1);
		dataSelection.addDataset(id2);

		Map<String, String> map = dataSelection.getParameters();
		String mapIds = map.get("datasetIds");

		assertEquals("1,2", mapIds);
	}

	@Test
	public void addDataSetsTest() {
		DataSelection dataSelection = new DataSelection();

		List<Long> ids = new ArrayList<Long>();
		Long id1 = new Long(1);
		Long id2 = new Long(2);
		ids.add(id1);
		ids.add(id2);

		dataSelection.addDatasets(ids);

		Map<String, String> map = dataSelection.getParameters();
		String mapIds = map.get("datasetIds");

		assertEquals("1,2", mapIds);
	}

	@Test
	public void addDataInvestigationTest() {
		DataSelection dataSelection = new DataSelection();
		Long id1 = new Long(1);
		Long id2 = new Long(2);

		dataSelection.addInvestigation(id1);
		dataSelection.addInvestigation(id2);

		Map<String, String> map = dataSelection.getParameters();
		String mapIds = map.get("investigationIds");

		assertEquals("1,2", mapIds);
	}

	@Test
	public void addDataInvestigationsTest() {
		DataSelection dataSelection = new DataSelection();

		List<Long> ids = new ArrayList<Long>();
		Long id1 = new Long(1);
		Long id2 = new Long(2);
		ids.add(id1);
		ids.add(id2);

		dataSelection.addInvestigations(ids);

		Map<String, String> map = dataSelection.getParameters();
		String mapIds = map.get("investigationIds");

		assertEquals("1,2", mapIds);
	}

	@Test
	public void dataSelectionTest() {
		DataSelection dataSelection = new DataSelection();
		Long df_id = new Long(1);
		Long ds_id = new Long(2);
		Long in_id = new Long(3);

		dataSelection.addDatafile(df_id);
		dataSelection.addDataset(ds_id);
		dataSelection.addInvestigation(in_id);

		Map<String, String> map = dataSelection.getParameters();
		String df_map_ids = map.get("datafileIds");
		String ds_map_ids = map.get("datasetIds");
		String in_map_ids = map.get("investigationIds");

		assertEquals("1", df_map_ids);
		assertEquals("2", ds_map_ids);
		assertEquals("3", in_map_ids);

	}

	@Test
	public void dataSelectionTest2() {
		DataSelection dataSelection = new DataSelection();
		Long df_id = new Long(1);
		Long ds_id = new Long(2);
		Long in_id = new Long(3);

		List<Long> df_ids = new ArrayList<Long>();
		List<Long> ds_ids = new ArrayList<Long>();
		List<Long> in_ids = new ArrayList<Long>();

		df_ids.add(df_id);
		ds_ids.add(ds_id);
		in_ids.add(in_id);

		dataSelection.addDatafile(df_id);
		dataSelection.addDatafiles(df_ids);
		dataSelection.addDataset(ds_id);
		dataSelection.addDatasets(ds_ids);
		dataSelection.addInvestigation(in_id);
		dataSelection.addInvestigations(in_ids);

		Map<String, String> map = dataSelection.getParameters();
		String df_map_ids = map.get("datafileIds");
		String ds_map_ids = map.get("datasetIds");
		String in_map_ids = map.get("investigationIds");

		assertEquals("1", df_map_ids);
		assertEquals("2", ds_map_ids);
		assertEquals("3", in_map_ids);
	}

}
