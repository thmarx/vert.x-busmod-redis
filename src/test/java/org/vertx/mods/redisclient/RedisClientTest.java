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

package org.vertx.mods.redisclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.framework.TestBase;
import org.vertx.mods.redis.CommandContext;
import org.vertx.mods.redis.RedisClient;
import org.vertx.mods.redis.commands.CommandException;
import org.vertx.mods.redis.commands.strings.GetCommand;
import org.vertx.mods.redisclient.commands.CommandTest;
import org.vertx.mods.redisclient.commands.CommandTest.TestMessage;

import redis.clients.jedis.Jedis;

/**
 * 
 * Most of the testing is done in JS since it's so much easier to play with JSON
 * in JS rather than Java
 * 
 * @author <a href="http://marx-labs.de">Thorsten Marx</a>
 */
public class RedisClientTest extends CommandTest {

	public static RedisClient client = new RedisClient();
	static Jedis jedis; 
	static CommandContext context;
	
	@BeforeClass
	public static void setUp () {
		jedis = new Jedis("localhost");
		
		client.setClient(jedis);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		jedis.quit();
	}
	
	@Test
	public void testGetCommand () throws CommandException {
		String key = getUniqueString();
		String value = getUniqueString();
		
		jedis.set(key, value);
		
		JsonObject request = new JsonObject();
		request.putString("key", key);
		request.putString("command", "get");
		
		TestMessage msg = getMessage(request);
		
		client.handle(msg);
		
		String reply = msg.reply.getString("value");
		assertNotNull(reply);
		assertEquals(value, reply);
	}
}
