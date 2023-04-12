const express = require("express");
const fs = require("fs");

const app = express();

const port = 8080;

//read data.json

app.get("/api/user", (req, res) => {
  const jsonData = JSON.parse(fs.readFileSync("Names.json", "utf8"));
  res.send(jsonData);
});

app.get("/api/q/health", (req, res) => {
  res.send("OK");
});

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});
