package com.luckyakhi.hivedemo.svc;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luckyakhi.hivedemo.domain.PartitionInfo;

public class HivePartitionExtractorTest {

	@Mock
	private HiveDAO hiveDAO;
	
	private PartitionExtractor hivePartitionExtractor;
	
	@Before
	public void setUp() throws Exception {
		initMocks(this);
		hivePartitionExtractor= new HivePartitionExtractor();
		ReflectionTestUtils.setField(hivePartitionExtractor, "hiveDAO", hiveDAO);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testExtract() throws JsonProcessingException {
		when(hiveDAO.getPartitions("ecoshuttle", "bookings")).thenReturn(
				new HashSet<>(Arrays.asList("country=in/state=ka/city=blr",
						"country=in/state=ka/city=mysore",
						"country=in/state=mh/city=mum","country=in/state=mh/city=pune")));
		Set<PartitionInfo> partitions=hivePartitionExtractor.extract("ecoshuttle", "bookings");
		ObjectMapper om = new ObjectMapper();
		System.out.println(om.writeValueAsString(partitions));
	}

}
