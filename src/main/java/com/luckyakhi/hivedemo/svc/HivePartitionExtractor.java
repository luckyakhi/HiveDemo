package com.luckyakhi.hivedemo.svc;

import static java.util.stream.Collectors.*;
import static org.apache.commons.collections.CollectionUtils.*;
import static org.apache.commons.lang.StringUtils.*;

import java.util.Map;
import java.util.Set;
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
		if(isEmpty(partitions) || isSetContainingNull(partitions)) return null;
		Map<Object, Set<String>> partitionMap=partitions.stream().collect(groupingBy(
				partition->partition.indexOf("/")==-1?partition:partition.substring(0,partition.indexOf(PARENT_CHILD_SEPARATOR)),
		mapping(value->value.indexOf("/")==-1?null:(String)(value.substring(value.indexOf(PARENT_CHILD_SEPARATOR)+1,
				value.length())), toSet())));
		Set<PartitionInfo> partitionSet=partitionMap.keySet().stream().map(key -> 
		((String)key).indexOf(KEY_VALUE_SEPARATOR)==-1?null: new PartitionInfo(
				((String)key).split(KEY_VALUE_SEPARATOR)[0],((String)key).split(KEY_VALUE_SEPARATOR)[1]))
				.collect(toSet());
		for (PartitionInfo partitionInfo : partitionSet) {
			Set<String> values = partitionMap.get(partitionInfo.getKey()
					+KEY_VALUE_SEPARATOR+partitionInfo.getValue());
			if(isNotEmpty(values) && !isSetContainingNull(values)){
				Set<PartitionInfo> children = getPartitionInfo(values);
				partitionInfo.setChildren(children);
			
			}
		}
		return partitionSet;
	}

	private boolean isSetContainingNull(Set<String> values) {
		return values.size()==1 && values.contains(null);
	}
	
	
}
