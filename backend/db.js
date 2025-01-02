import { Client } from "cassandra-driver";
import dotenv from "dotenv";

dotenv.config({});

const client = new Client({
  cloud: {
    secureConnectBundle: process.env.DB_SECURE_BUNDLE_PATH,
  },
  credentials: {
    username: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
  },
});

(async () => {
  try {
    await client.connect();
    console.log("Connected to Astra DB successfully.");

    const query = `
      INSERT INTO engagement.engagement (post_id, type, likes, shares, comments) 
      VALUES (?, ?, ?, ?, ?);
    `;

    const dataset = [
      { post_id: 1, type: "carousel", likes: 120, shares: 30, comments: 20 },
      { post_id: 2, type: "reel", likes: 300, shares: 50, comments: 80 },
      { post_id: 3, type: "static", likes: 90, shares: 10, comments: 5 },
    ];

    for (const data of dataset) {
      await client.execute(query, Object.values(data), { prepare: true });
      console.log("Inserted:", data);
    }
  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.shutdown();
  }
})();
