/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.ml.vertx.mods.redis;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import net.ml.vertx.mods.redis.commands.Command;
import net.ml.vertx.mods.redis.commands.CommandException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

/**
 * RedisClient Bus Module
 * <p>
 * Please see the busmods manual for a full description
 * <p>
 * 
 * @author <a href="http://marx-labs.de">Thorsten Marx</a>
 */
public class RedisClient extends BusModBase implements
		Handler<Message<JsonObject>>, CommandContext {

	private static final Map<String, Command> commands = new HashMap<String, Command>();

	static {
		if (commands.isEmpty()) {
			ServiceLoader<Command> commandloader = ServiceLoader.load(Command.class, Command.class.getClassLoader());
			Iterator<Command> iter = commandloader.iterator(); 
			while (iter.hasNext()) {
				Command c = iter.next();
				commands.put(c.getName(), c);
			}
		}
	}
	
	private String address;
	private String host;
	private int port;
	private Jedis redis;

	@Override
	public void start() {
		super.start();
		
		System.out.println("START");
		if (commands.isEmpty()) {
			ServiceLoader<Command> commandloader = ServiceLoader.load(Command.class, this.getClass().getClassLoader());
			Iterator<Command> iter = commandloader.iterator(); 
			while (iter.hasNext()) {
				Command c = iter.next();
				commands.put(c.getName(), c);
			}
		}
		connect();
	}

	@Override
	public void stop() {
		if (redis != null) {
			redis.quit();
		}
	}
	
	private void connect () {
		
		address = getOptionalStringConfig("address", "vertx.redis-client");
		host = getOptionalStringConfig("host", "localhost");
		port = getOptionalIntConfig("port", 6379);

		try {
			redis = new Jedis(host, port);
			redis.ping();
			eb.registerHandler(address, this);
		} catch (JedisException e) {
			logger.error("Failed to connect to redis server", e);
		}
	}

	public void handle(Message<JsonObject> message) {
		String command = message.body.getString("command");
		
		if (command == null) {
			sendError(message, "command must be specified");
			return;
		}
		
		Command commandHandler = commands.get(command.toLowerCase());

		if (commandHandler != null) {
			try {
				commandHandler.handle(message, this);
			} catch (CommandException e) {
				sendError(message, e.getMessage());
			}
		} else {
			sendError(message, "Invalid command: " + command);
			return;
		}
	}

	@Override
	public Jedis getClient() {
		return redis;
	}
	
	public void setClient (Jedis client) {
		this.redis = client;
	}
}
