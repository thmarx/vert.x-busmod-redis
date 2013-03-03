load('vertx.js');

var eb = vertx.eventBus;

var names = [
  { id: "1",
    name: "John"
  },
  { id: "2",
    name: "Paul"
  },
  { id: "3",
    name: "George"
  },
  { id: "4",
    name: "Ringo"
  }
];

eb.send("test.persistor", {
  command: "del",
  keys: ["test:name", "test:names", "test:names:1", "test:names:2", "test:names:3", "test:names:4"]
});

for(var i=0; i < names.length; i++){
  eb.send("test.persistor",
    { command: "sadd",
      key: "test:names",
      members: [ names[i].id ]
    }
  );
  eb.send("test.persistor",
    { command: "set",
      key: "test:names:" + names[i].id,
      value: names[i].name
    }
  );
}
