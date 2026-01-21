const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json());

// Config (DO NOT hardcode in future – env friendly)
const REST_PROXY = process.env.REST_PROXY || "http://rest-proxy:8082";
const TOPIC = process.env.TOPIC || "notifications";
const PORT = process.env.PORT || 3000;

// Required fields
const REQUIRED_FIELDS = [
  "name",
  "bloodType",
  "units",
  "contact",
  "urgency",
  "timestamp",
];

// 🔹 Validation function
function validatePayload(payload) {
  const missing = REQUIRED_FIELDS.filter(
    (field) => !payload.hasOwnProperty(field)
  );
  return missing;
}

// 🔹 Producer API
app.post("/notify", async (req, res) => {
  const payload = req.body;

  const missingFields = validatePayload(payload);
  if (missingFields.length > 0) {
    return res.status(400).json({
      error: "Missing required fields",
      missingFields,
    });
  }

  try {
    const kafkaResponse = await axios.post(
      `${REST_PROXY}/topics/${TOPIC}`,
      {
        records: [{ value: payload }],
      },
      {
        headers: {
          "Content-Type": "application/vnd.kafka.json.v2+json",
        },
      }
    );

    res.status(200).json({
      message: "Notification sent successfully",
      kafkaResponse: kafkaResponse.data,
    });
  } catch (err) {
    console.error("Kafka produce error:", err.response?.data || err.message);
    res.status(500).json({
      error: "Failed to send notification",
    });
  }
});

// 🔹 Health check
app.get("/health", (req, res) => {
  res.json({ status: "Producer API running" });
});

// Start server
app.listen(PORT, () => {
  console.log(`Notification Producer running on port ${PORT}`);
});
