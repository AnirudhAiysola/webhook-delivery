import express, { Request, Response } from "express";
import "dotenv/config";

const app = express();

app.use(express.json());

const PORT = process.env.PORT || 3000;

app.get("/health", (req: Request, res: Response) => {
  res.json({ message: "STATUS OK 200" });
});

app.listen(PORT, () => {
  console.log("Server started on PORT 3000");
});
