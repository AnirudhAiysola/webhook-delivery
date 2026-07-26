import express, { Request, Response } from "express";
import "dotenv/config";

import { insertEvent } from "./events";
import { isValidWebhookUrl, insertSubscription } from "./subscriptions";

const app = express();

app.use(express.json());

const PORT = process.env.PORT || 3000;

app.get("/health", (req: Request, res: Response) => {
  res.json({ message: "STATUS OK 200" });
});

app.post("/events", async (req: Request, res: Response) => {
  try {
    const { eventType, payload, key } = req.body;

    // validation check
    if (!eventType || !key) {
      return res.status(400).json({ error: "eventType and key are required" });
    }

    const result = await insertEvent({ eventType, payload, key });

    if (result.created) {
      return res.status(202).json({ id: result.id });
    }

    return res.status(200).json({ message: "Event already received" });
  } catch (error) {
    console.error("Error inserting event", error);
    return res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/subscription", async (req: Request, res: Response) => {
  try {
    const { eventType, url } = req.body;

    // validation check
    if (!eventType || !url) {
      return res.status(400).json({ error: "eventType and url are required" });
    }
    // valid url check
    if (!isValidWebhookUrl(url)) {
      return res.status(400).json({ error: "url must be a valid http(s) URL" });
    }
    const { id } = await insertSubscription({ eventType, url });
    return res.status(201).json({ id });
  } catch (error) {
    console.error("Error inserting subscription", error);
    return res.status(500).json({ error: "Internal server error" });
  }
});

app.listen(PORT, () => {
  console.log("Server started on PORT 3000");
});
