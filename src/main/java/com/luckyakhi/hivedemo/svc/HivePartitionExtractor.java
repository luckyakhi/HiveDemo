package com.luckyakhi.hivedemo.svc;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.luckyakhi.hivedemo.domain.PartitionInfo;

@Component
public class HivePartitionExtractor implements PartitionExtractor{
	
	private static final Logger log = LoggerFactory.getLogger(HivePartitionExtractor.class);
	private static final String PARENT_CHILD_SEPARATOR = "/";
	private static final String KEY_VALUE_SEPARATOR = "=";
	
	@Autowired
	private HiveDAO hiveDAO;

	public Set<PartitionInfo> extract(String databaseName, String tableName) {
		log.info("Getting partitions for database {} and table {}", databaseName,tableName);
		Set<String> partitions = hiveDAO.getPartitions(databaseName, tableName);
		return getPartitionInfo(partitions);
	}

	private Set<PartitionInfo> getPartitionInfo(Set<String> partitions) {
		if(CollectionUtils.isEmpty(partitions)) return null;
		Map<Object, Set<String>> partitionMap=partitions.stream().collect(Collectors.groupingBy(partition->
		partition.substring(0,partition.indexOf(PARENT_CHILD_SEPARATOR)),
		Collectors.mapping(value->(String)(value.substring(value.indexOf(PARENT_CHILD_SEPARATOR)+1,
				value.length())), Collectors.toSet())));
		Set<PartitionInfo> partitionSet=partitionMap.keySet().stream().map(key -> new PartitionInfo(
				((String)key).split(KEY_VALUE_SEPARATOR)[0],((String)key).split(KEY_VALUE_SEPARATOR)[1]))
				.collect(Collectors.toSet());
		for (PartitionInfo partitionInfo : partitionSet) {
			partitionInfo.setChildren(getPartitionInfo(partitionMap.get(partitionInfo.getKey()
					+KEY_VALUE_SEPARATOR+partitionInfo.getValue())));
		}
		return partitionSet;
	}
	
	
}
