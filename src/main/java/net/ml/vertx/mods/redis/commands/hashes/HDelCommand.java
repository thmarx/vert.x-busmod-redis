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
package net.ml.vertx.mods.redis.commands.hashes;

import java.util.concurrent.Future;

import net.ml.vertx.mods.redis.CommandContext;
import net.ml.vertx.mods.redis.commands.Command;
import net.ml.vertx.mods.redis.commands.CommandException;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * HDelCommand
 * <p>
 * 
 * @author <a href="http://marx-labs.de">Thorsten Marx</a>
 */
public class HDelCommand extends Command {

	public static final String COMMAND = "hdel";

	public HDelCommand () {
		super(COMMAND);
	}
	
	@Override
	public void handle(final Message<JsonObject> message, CommandContext context) throws CommandException {
		
		String key = getMandatoryString("key", message);
		checkNull(key, "key can not be null");
		
		
		JsonArray fields = message.body.getArray("fields");
		
		if (fields == null) {
			throw new CommandException("fields can not be null");
		}
		
		try {
			final Future<Long> response = context.getConnection().hdel(key, getStringArray(fields));
			

			vertx.runOnLoop(new Handler<Void>() {
				
				@Override
				public void handle(Void event) {
					try {
						response(message, response.get());
					} catch (Exception e) {
						sendError(message, e.getLocalizedMessage());
					}
				}
			});
		} catch (Exception e) {
			sendError(message, e.getLocalizedMessage());
		}

	}
}
