package com.luckyakhi.hivedemo.domain;

import java.util.Set;

public class PartitionInfo {
	private String key;
	private String value;
	private Set<PartitionInfo> children;
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public Set<PartitionInfo> getChildren() {
		return children;
	}
	public void setChildren(Set<PartitionInfo> children) {
		this.children = children;
	}
	
	public PartitionInfo(String key, String value) {
		super();
		this.key = key;
		this.value = value;
	}
	public PartitionInfo() {
		super();
	}
	@Override
	public String toString() {
		return "PartitionInfo [key=" + key + ", value=" + value + ", children=" + children + "]";
	}
	
	
}
