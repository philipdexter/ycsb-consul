/**
 * Copyright (c) 2013 - 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.option.ConsistencyMode;
import com.orbitz.consul.option.ImmutableQueryOptions;
import com.orbitz.consul.option.QueryOptions;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.net.HostAndPort;

public class ConsulClient extends DB {
	public static final String HOSTS_PROPERTY = "hosts";
	public static final String PORT_PROPERTY = "port";
	public static final String PORT_PROPERTY_DEFAULT = "8888";
	public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "consul.readconsistencylevel";
	public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = null; // DEFAULT(null), STALE("stale"),
																				// CONSISTENT("consistent");

	ArrayList<KeyValueClient> clients = new ArrayList<KeyValueClient>(); // round robin over client list
	protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();

	private long roundrobin = 0;
	private QueryOptions additionalOptions;

	/* init will be called per client thread */
	@Override
	public void init() throws DBException {
		Properties props = getProperties();
		String host = getProperties().getProperty(HOSTS_PROPERTY);
		if (host == null) {
			throw new DBException(String.format("Required property \"%s\" missing for Consul", HOSTS_PROPERTY));
		}
		String[] hosts = host.split(",");
		String port = props.getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);
		String readConsistency = props.getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
				READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT);
		for (String destHost : hosts) {
			KeyValueClient client = Consul.builder()
					.withHostAndPort(HostAndPort.fromParts(destHost, Integer.valueOf(port))).build().keyValueClient();
			clients.add(client);
		}
		additionalOptions = ImmutableQueryOptions.builder().consistencyMode(getConsistencyMode(readConsistency))
				.build();
	}

	@Override
	public void cleanup() throws DBException {
	}

	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
		int clientIndex = (int) ((roundrobin++) % clients.size());
		Optional<Value> value = clients.get(clientIndex).getValue(table + "." + key, additionalOptions);
		if (value.isEmpty() || value.get().getValue().isEmpty()) {
			return Status.ERROR;
		}
		decode(value.get().getValue().get(), fields, result);
		return Status.OK;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		return null;
	}

	@Override
	public Status update(String table, String key, Map<String, ByteIterator> values) {
		return insert(table, key, values);
	}

	@Override
	public Status insert(String table, String key, Map<String, ByteIterator> values) {
		int clientIndex = (int) ((roundrobin++) % clients.size());
		if (clients.get(clientIndex).putValue(table + "." + key, encode(values))) {
			return Status.OK;
		}
		return Status.ERROR;
	}

	@Override
	public Status delete(String table, String key) {
		int clientIndex = (int) ((roundrobin++) % clients.size());
		clients.get(clientIndex).deleteKey(table + "." + key);
		return Status.OK;
	}

	/**
	 * Decode the object from server into the storable result.
	 *
	 * @param source the loaded object.
	 * @param fields the fields to check.
	 * @param dest   the result passed back to the ycsb core.
	 */
	private void decode(final String source, final Set<String> fields, final Map<String, ByteIterator> dest) {

		try {
			JsonNode json = JSON_MAPPER.readTree((String) source);
			boolean checkFields = fields != null && !fields.isEmpty();
			for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
				Map.Entry<String, JsonNode> jsonField = jsonFields.next();
				String name = jsonField.getKey();
				if (checkFields && fields.contains(name)) {
					continue;
				}
				JsonNode jsonValue = jsonField.getValue();
				if (jsonValue != null && !jsonValue.isNull()) {
					dest.put(name, new StringByteIterator(jsonValue.asText()));
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Could not decode JSON");
		}
	}

	/**
	 * Encode the object for consul storage.
	 *
	 * @param source the source value.
	 * @return the storable object.
	 */
	private String encode(final Map<String, ByteIterator> source) {
		Map<String, String> stringMap = StringByteIterator.getStringMap(source);

		ObjectNode node = JSON_MAPPER.createObjectNode();
		for (Map.Entry<String, String> pair : stringMap.entrySet()) {
			node.put(pair.getKey(), pair.getValue());
		}
		JsonFactory jsonFactory = new JsonFactory();
		Writer writer = new StringWriter();
		try {
			JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer);
			JSON_MAPPER.writeTree(jsonGenerator, node);
		} catch (Exception e) {
			throw new RuntimeException("Could not encode JSON value");
		}
		return writer.toString();
	}

	ConsistencyMode getConsistencyMode(String value) {
		if (value == null) {
			return ConsistencyMode.DEFAULT;
		}
		return ConsistencyMode.valueOf(value);
	}

}
