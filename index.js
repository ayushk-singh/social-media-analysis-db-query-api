import express from "express";
import { Client } from "cassandra-driver";
import dotenv from 'dotenv'

dotenv.config({})

const app = express();
const port = 3000;

const client = new Client({
  cloud: {
    secureConnectBundle: process.env.DB_SECURE_BUNDLE_PATH,
  },
  credentials: {
    username: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
  },
});

client.connect();

app.get("/aggregate", async (req, res) => {
  try {
    const query = "SELECT type, likes, shares, comments FROM engagement.engagement;";
    const result = await client.execute(query);
    
    // Check if result is empty
    if (result.rows.length === 0) {
      return res.status(404).send("No data found.");
    }

    const averages = result.rows.reduce((acc, { type, likes, shares, comments }) => {
      if (!acc[type]) {
        acc[type] = { likes: 0, shares: 0, comments: 0, count: 0 };
      }
      acc[type].likes += likes;
      acc[type].shares += shares;
      acc[type].comments += comments;
      acc[type].count += 1;
  
      return acc;
    }, {});

    // Calculate average for each type
    const resultData = [];
    for (const type in averages) {
      const { likes, shares, comments, count } = averages[type];
      resultData.push({
        type,
        avg_likes: likes / count,
        avg_shares: shares / count,
        avg_comments: comments / count,
      });
    }

    // Send the results as JSON response
    res.json(resultData);
  } catch (error) {
    console.error(error);
    res.status(500).send("Error retrieving data from the database.");
  }
});

app.listen(port, () => console.log(`API running on http://localhost:${port}`));
