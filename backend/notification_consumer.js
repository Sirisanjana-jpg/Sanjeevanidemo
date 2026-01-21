const express = require("express");
const axios = require("axios");

const app = express();

// Config
const REST_PROXY = process.env.REST_PROXY || "http://rest-proxy:8082";
const CONSUMER_GROUP = "notification-consumers";
const CONSUMER_INSTANCE = "notification-consumer-1";
const TOPIC = "notifications";
const PORT = process.env.PORT || 4000;

// In-memory store (temporary)
let notifications = [];

/**
 * Wait for REST Proxy to be ready
 */
async function waitForRestProxy() {
  while (true) {
    try {
      await axios.get(`${REST_PROXY}/brokers`);
      console.log("Kafka REST Proxy is ready");
      break;
    } catch {
      console.log("Waiting for Kafka REST Proxy...");
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
}

/**
 * Create Kafka REST consumer
 */
async function createConsumer() {
  // 1️⃣ Create consumer instance
  await axios.post(
    `${REST_PROXY}/consumers/${CONSUMER_GROUP}`,
    {
      name: CONSUMER_INSTANCE,
      format: "json",           // must match producer
      "auto.offset.reset": "earliest",
    },
    {
      headers: {
        "Content-Type": "application/vnd.kafka.v2+json",
      },
    }
  );

  // 2️⃣ Subscribe to topic
  await axios.post(
    `${REST_PROXY}/consumers/${CONSUMER_GROUP}/instances/${CONSUMER_INSTANCE}/subscription`,
    { topics: [TOPIC] },
    {
      headers: {
        "Content-Type": "application/vnd.kafka.v2+json",
      },
    }
  );

  console.log("Notification consumer created & subscribed to topic:", TOPIC);
}

/**
 * Poll Kafka for new messages
 */
async function pollKafka() {
  try {
    const res = await axios.get(
      `${REST_PROXY}/consumers/${CONSUMER_GROUP}/instances/${CONSUMER_INSTANCE}/records`,
      {
        headers: {
          Accept: "application/vnd.kafka.json.v2+json",
        },
      }
    );

    if (res.data.length > 0) {
      res.data.forEach((record) => {
        notifications.push({
          ...record.value,
          receivedAt: new Date(),
        });
      });
      console.log(`Received ${res.data.length} notifications`);
    }
  } catch (err) {
    console.error("Polling error:", err.response?.data || err.message);
  }
}

/**
 * GET endpoint for devices to fetch notifications
 */
app.get("/notifications", (req, res) => {
  res.json({
    count: notifications.length,
    notifications,
  });
});

/**
 * Optional DELETE endpoint to clear notifications
 */
app.delete("/notifications", (req, res) => {
  notifications = [];
  res.json({ message: "Notifications cleared" });
});

/**
 * Health check
 */
app.get("/health", (req, res) => {
  res.json({ status: "Notification consumer running" });
});

/**
 * Startup
 */
async function start() {
  await waitForRestProxy();

  try {
    await createConsumer();
    setInterval(pollKafka, 3000); // poll every 3s
    app.listen(PORT, () =>
      console.log(`Notification Consumer running on port ${PORT}`)
    );
  } catch (err) {
    console.error("Consumer startup failed:", err.response?.data || err.message);
  }
}

start();
