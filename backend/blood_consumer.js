const axios = require("axios");
const { MongoClient } = require("mongodb");

const REST_PROXY = process.env.REST_PROXY || "http://rest-proxy:8082";
const CONSUMER_GROUP = "blood-consumers";
const CONSUMER_INSTANCE = "consumer_instance_1";
const TOPIC = "blood-requests";

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB;
const COLLECTION_NAME = process.env.MONGO_COLLECTION;

let collection;

const kafkaHeaders = {
  headers: {
    "Content-Type": "application/vnd.kafka.v2+json",
    "Accept": "application/vnd.kafka.v2+json"
  }
};

// ---------- Mongo ----------
async function connectMongo() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  console.log("Connected to MongoDB");

  const db = client.db(DB_NAME);
  const collections = await db.listCollections({ name: COLLECTION_NAME }).toArray();

  if (collections.length === 0) {
    await db.createCollection(COLLECTION_NAME);
    console.log(`Collection '${COLLECTION_NAME}' created`);
  } else {
    console.log(`Collection '${COLLECTION_NAME}' already exists`);
  }

  collection = db.collection(COLLECTION_NAME);
}

// ---------- Wait for REST Proxy ----------
async function waitForRestProxy() {
  while (true) {
    try {
      await axios.get(`${REST_PROXY}/topics`, kafkaHeaders);
      console.log("Kafka REST Proxy is ready");
      break;
    } catch {
      console.log("Waiting for Kafka REST Proxy...");
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

// ---------- Consumer ----------
async function createConsumer() {
  await axios.post(
    `${REST_PROXY}/consumers/${CONSUMER_GROUP}`,
    {
      name: CONSUMER_INSTANCE,
      format: "json",
      "auto.offset.reset": "earliest"
    },
    kafkaHeaders
  );

  await axios.post(
    `${REST_PROXY}/consumers/${CONSUMER_GROUP}/instances/${CONSUMER_INSTANCE}/subscription`,
    { topics: [TOPIC] },
    kafkaHeaders
  );

  console.log("Consumer created & subscribed");
}

// ---------- Poll ----------
function pollMessages() {
  setInterval(async () => {
    try {
      const res = await axios.get(
        `${REST_PROXY}/consumers/${CONSUMER_GROUP}/instances/${CONSUMER_INSTANCE}/records`,
         {
    headers: {
      "Accept": "application/vnd.kafka.json.v2+json"
    }
  }
      );

      for (const record of res.data) {
        console.log("New Blood Request:", record.value);

        await collection.insertOne({
          ...record.value,
          receivedAt: new Date()
        });

        console.log("Inserted into MongoDB");
      }
    } catch (err) {
      console.error("Polling error:", err.response?.data || err.message);
    }
  }, 3000);
}

// ---------- Startup ----------
(async () => {
  try {
    await connectMongo();
    await waitForRestProxy();
    await createConsumer();
    pollMessages();
  } catch (err) {
    console.error("Consumer startup failed:", err.response?.data || err.message);
    process.exit(1);
  }
})();
