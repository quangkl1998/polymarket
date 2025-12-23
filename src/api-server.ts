import express from "express";
import { promises as fs } from "fs";
import path from "path";
import * as XLSX from "xlsx";
import swaggerUi from "swagger-ui-express";
import swaggerJsdoc from "swagger-jsdoc";

const app = express();
const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;

// Enable CORS
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

app.use(express.json());

// Request timeout middleware (30 seconds)
app.use((req, res, next) => {
  req.setTimeout(30000, () => {
    if (!res.headersSent) {
      res.status(504).json({ error: "Request timeout" });
    }
  });
  next();
});

// Request logging middleware
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} ${req.method} ${req.path}`);
  next();
});

// Error handling middleware
app.use(
  (
    err: any,
    req: express.Request,
    res: express.Response,
    next: express.NextFunction
  ) => {
    console.error("Unhandled error:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: err.message || "Internal server error" });
    }
  }
);

const DATA_DIR = path.join(process.cwd(), "data");
const WALLETS_DIR = path.join(DATA_DIR, "wallets");
const MARKETS_DIR = path.join(DATA_DIR, "markets");

// Swagger configuration
const swaggerOptions: swaggerJsdoc.Options = {
  definition: {
    openapi: "3.0.0",
    info: {
      title: "Polymarket Data API",
      version: "1.0.0",
      description: "API for accessing Polymarket wallet and market data",
    },
    servers: [
      {
        url: `http://localhost:${PORT}`,
        description: "Development server",
      },
    ],
  },
  apis: ["./src/api-server.ts"], // Path to the API files
};

const swaggerSpec = swaggerJsdoc(swaggerOptions);

// Swagger UI
app.use("/api-docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec));

/**
 * @swagger
 * /api/wallets:
 *   get:
 *     summary: Get all wallets
 *     tags: [Wallets]
 *     responses:
 *       200:
 *         description: List of all wallets
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 wallets:
 *                   type: array
 *                   items:
 *                     type: object
 *                     properties:
 *                       name:
 *                         type: string
 *                       address:
 *                         type: string
 *                       folder:
 *                         type: string
 *                       markets:
 *                         type: array
 *                         items:
 *                           type: string
 *                       updatedAt:
 *                         type: string
 */
app.get("/api/wallets", async (req, res) => {
  console.log(
    "GET /api/wallets - Request received at",
    new Date().toISOString()
  );

  const startTime = Date.now();

  try {
    // Immediately send headers to prevent timeout
    res.setHeader("Content-Type", "application/json");

    const wallets: any[] = [];

    // Check if wallets directory exists
    try {
      await fs.access(WALLETS_DIR);
      console.log(`Wallets directory exists: ${WALLETS_DIR}`);
    } catch (err: any) {
      // Directory doesn't exist, return empty array immediately
      console.log(`Wallets directory does not exist, returning empty array`);
      return res.json({ wallets: [] });
    }

    // Read wallet directories
    let walletDirs;
    try {
      walletDirs = await fs.readdir(WALLETS_DIR, { withFileTypes: true });
      console.log(`Found ${walletDirs.length} items in wallets directory`);
    } catch (err: any) {
      console.error("Error reading wallets directory:", err);
      return res.status(500).json({
        error: `Failed to read wallets directory: ${err.message}`,
      });
    }

    // Process wallets sequentially to avoid overwhelming the system
    for (const dir of walletDirs) {
      if (dir.isDirectory()) {
        try {
          const walletPath = path.join(WALLETS_DIR, dir.name);
          const metadataFile = path.join(walletPath, ".wallet.json");

          let metadata: any = { name: dir.name };
          try {
            const metadataContent = await fs.readFile(metadataFile, "utf-8");
            metadata = JSON.parse(metadataContent);
          } catch {
            // Metadata file doesn't exist, use folder name
          }

          // Get markets for this wallet (markets are now folders)
          let marketDirs: string[] = [];
          try {
            const items = await fs.readdir(walletPath, { withFileTypes: true });
            marketDirs = items
              .filter((item) => item.isDirectory())
              .map((item) => item.name);
          } catch (err) {
            // Ignore errors reading wallet directory
            console.warn(`Failed to read markets for wallet ${dir.name}:`, err);
          }

          wallets.push({
            name: metadata.name || dir.name,
            address: metadata.address || dir.name,
            folder: dir.name,
            markets: marketDirs,
            updatedAt: metadata.updatedAt,
          });
        } catch (err) {
          console.warn(`Error processing wallet ${dir.name}:`, err);
        }
      }
    }

    console.log(
      `GET /api/wallets - Returning ${wallets.length} wallets (took ${
        Date.now() - startTime
      }ms)`
    );
    res.json({ wallets });
  } catch (err: any) {
    console.error("Error in /api/wallets:", err);
    if (!res.headersSent) {
      res.status(500).json({ error: err.message || "Internal server error" });
    }
  }
});

/**
 * Get markets for a specific wallet
 * Markets are now folders, not files
 */
app.get("/api/wallets/:walletName/markets", async (req, res) => {
  try {
    const { walletName } = req.params;
    const walletPath = path.join(WALLETS_DIR, walletName);

    try {
      await fs.access(walletPath);
    } catch {
      return res.status(404).json({ error: "Wallet not found" });
    }

    const items = await fs.readdir(walletPath, { withFileTypes: true });
    const marketDirs = items.filter((item) => item.isDirectory());

    const markets = marketDirs.map((dir) => ({
      name: dir.name,
      folder: dir.name,
    }));

    res.json({ wallet: walletName, markets });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/wallets/{walletName}/markets/{market}/sessions:
 *   get:
 *     summary: Get all sessions for a wallet and market
 *     tags: [Wallets]
 *     parameters:
 *       - in: path
 *         name: walletName
 *         required: true
 *         schema:
 *           type: string
 *       - in: path
 *         name: market
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: List of sessions
 *       404:
 *         description: Market file not found
 */
app.get(
  "/api/wallets/:walletName/markets/:market/sessions/:session",
  async (req, res) => {
    try {
      const { walletName, market, session } = req.params;
      const marketPath = path.join(WALLETS_DIR, walletName, market);

      try {
        await fs.access(marketPath);
      } catch {
        return res.status(404).json({ error: "Market folder not found" });
      }

      const files = await fs.readdir(marketPath);
      const dateFiles = files.filter((f) => f.endsWith(".xlsx"));

      // Search for session across all date files
      let foundData: any[] = [];
      let foundDate = "";

      for (const dateFile of dateFiles) {
        try {
          const excelFile = path.join(marketPath, dateFile);
          const workbook = XLSX.readFile(excelFile);

          if (workbook.SheetNames.includes(session)) {
            const worksheet = workbook.Sheets[session];
            const data = XLSX.utils.sheet_to_json(worksheet);
            foundData = data;
            foundDate = dateFile.replace(".xlsx", "");
            break;
          }
        } catch (err) {
          // Continue searching
          continue;
        }
      }

      if (foundData.length === 0 && foundDate === "") {
        return res.status(404).json({ error: "Session not found" });
      }

      res.json({
        wallet: walletName,
        market,
        session,
        date: foundDate,
        count: foundData.length,
        trades: foundData,
      });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  }
);

/**
 * Get all dates (files) for a wallet and market
 */
app.get("/api/wallets/:walletName/markets/:market/dates", async (req, res) => {
  try {
    const { walletName, market } = req.params;
    const marketPath = path.join(WALLETS_DIR, walletName, market);

    try {
      await fs.access(marketPath);
    } catch {
      return res.status(404).json({ error: "Market folder not found" });
    }

    const files = await fs.readdir(marketPath);
    const dateFiles = files
      .filter((f) => f.endsWith(".xlsx"))
      .map((f) => f.replace(".xlsx", ""))
      .sort()
      .reverse(); // Most recent first

    res.json({
      wallet: walletName,
      market,
      dates: dateFiles,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * Get all sessions for a wallet and market
 * Now searches across all date files in the market folder
 */
app.get(
  "/api/wallets/:walletName/markets/:market/sessions",
  async (req, res) => {
    try {
      const { walletName, market } = req.params;
      const marketPath = path.join(WALLETS_DIR, walletName, market);

      try {
        await fs.access(marketPath);
      } catch {
        return res.status(404).json({ error: "Market folder not found" });
      }

      const files = await fs.readdir(marketPath);
      const dateFiles = files.filter((f) => f.endsWith(".xlsx"));

      const allSessions = new Map<
        string,
        { name: string; slug: string; date: string }
      >();

      // Collect sessions from all date files
      for (const dateFile of dateFiles) {
        try {
          const excelFile = path.join(marketPath, dateFile);
          const workbook = XLSX.readFile(excelFile);
          const date = dateFile.replace(".xlsx", "");

          workbook.SheetNames.forEach((sheetName) => {
            if (!allSessions.has(sheetName)) {
              allSessions.set(sheetName, {
                name: sheetName,
                slug: sheetName,
                date: date,
              });
            }
          });
        } catch (err) {
          // Skip files that can't be read
          console.warn(`Failed to read ${dateFile}:`, err);
        }
      }

      res.json({
        wallet: walletName,
        market,
        sessions: Array.from(allSessions.values()),
      });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  }
);

/**
 * @swagger
 * /api/markets:
 *   get:
 *     summary: Get all markets
 *     tags: [Markets]
 *     responses:
 *       200:
 *         description: List of all markets
 */
app.get("/api/markets", async (req, res) => {
  try {
    if (!(await fs.access(MARKETS_DIR).catch(() => null))) {
      return res.json({ markets: [] });
    }

    const files = await fs.readdir(MARKETS_DIR);
    const excelFiles = files.filter((f) => f.endsWith(".xlsx"));

    const markets = excelFiles.map((f) => {
      const slug = f.replace(".xlsx", "");
      return {
        slug,
        file: f,
      };
    });

    res.json({ markets });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/markets/{slug}/price-changes-jsonl:
 *   get:
 *     summary: Get price changes for a market (from JSONL)
 *     tags: [Markets]
 *     parameters:
 *       - in: path
 *         name: slug
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: List of price changes from JSONL
 *       404:
 *         description: Market JSONL file not found
 */
app.get("/api/markets/:slug/price-changes-jsonl", async (req, res) => {
  try {
    const { slug } = req.params;
    const jsonlFile = path.join(MARKETS_DIR, `${slug}.jsonl`);

    try {
      await fs.access(jsonlFile);
    } catch {
      return res.status(404).json({ error: "Market JSONL file not found" });
    }

    const content = await fs.readFile(jsonlFile, "utf-8");
    const lines = content
      .trim()
      .split("\n")
      .filter((line) => line.trim());

    const priceChanges = lines
      .map((line) => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      })
      .filter((obj) => obj && obj.type === "price_change");

    res.json({
      slug,
      count: priceChanges.length,
      price_changes: priceChanges,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/markets/{slug}/trades:
 *   get:
 *     summary: Get trades for a market
 *     tags: [Markets]
 *     parameters:
 *       - in: path
 *         name: slug
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: List of trades
 *       404:
 *         description: Market file not found
 */
app.get("/api/markets/:slug/trades", async (req, res) => {
  try {
    const { slug } = req.params;
    const excelFile = path.join(MARKETS_DIR, `${slug}.xlsx`);

    try {
      await fs.access(excelFile);
    } catch {
      return res.status(404).json({ error: "Market file not found" });
    }

    const workbook = XLSX.readFile(excelFile);

    if (!workbook.SheetNames.includes("trades")) {
      return res.json({
        slug,
        count: 0,
        trades: [],
      });
    }

    const worksheet = workbook.Sheets["trades"];
    const data = XLSX.utils.sheet_to_json(worksheet);

    res.json({
      slug,
      count: data.length,
      trades: data,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/markets/{slug}/orderbook-snapshot:
 *   get:
 *     summary: Get orderbook snapshots for a market
 *     tags: [Markets]
 *     parameters:
 *       - in: path
 *         name: slug
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: List of orderbook snapshots
 *       404:
 *         description: Market file not found
 */
app.get("/api/markets/:slug/orderbook-snapshot", async (req, res) => {
  try {
    const { slug } = req.params;
    const excelFile = path.join(MARKETS_DIR, `${slug}.xlsx`);

    try {
      await fs.access(excelFile);
    } catch {
      return res.status(404).json({ error: "Market file not found" });
    }

    const workbook = XLSX.readFile(excelFile);

    if (!workbook.SheetNames.includes("orderbook_snapshot")) {
      return res.json({
        slug,
        count: 0,
        snapshots: [],
      });
    }

    const worksheet = workbook.Sheets["orderbook_snapshot"];
    const data = XLSX.utils.sheet_to_json(worksheet);

    res.json({
      slug,
      count: data.length,
      snapshots: data,
    });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/markets/{slug}/price-changes/download:
 *   get:
 *     summary: Download price changes Excel file for a market
 *     tags: [Markets]
 *     parameters:
 *       - in: path
 *         name: slug
 *         required: true
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: Excel file with price changes
 *         content:
 *           application/vnd.openxmlformats-officedocument.spreadsheetml.sheet:
 *             schema:
 *               type: string
 *               format: binary
 *       404:
 *         description: Market file not found
 */
app.get("/api/markets/:slug/price-changes/download", async (req, res) => {
  try {
    const { slug } = req.params;
    const excelFile = path.join(MARKETS_DIR, `${slug}.xlsx`);

    try {
      await fs.access(excelFile);
    } catch {
      return res.status(404).json({ error: "Market file not found" });
    }

    const workbook = XLSX.readFile(excelFile);

    if (!workbook.SheetNames.includes("price_changes")) {
      return res.status(404).json({ error: "Price changes sheet not found" });
    }

    // Create a new workbook with only price_changes sheet
    const newWorkbook = XLSX.utils.book_new();
    const worksheet = workbook.Sheets["price_changes"];
    XLSX.utils.book_append_sheet(newWorkbook, worksheet, "price_changes");

    // Generate Excel buffer
    const excelBuffer = XLSX.write(newWorkbook, {
      type: "buffer",
      bookType: "xlsx",
    });

    // Set headers for file download
    const fileName = `${slug}-price-changes.xlsx`;
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
    res.setHeader("Content-Length", excelBuffer.length);

    res.send(excelBuffer);
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/wallets/{walletName}/markets/{market}/download:
 *   get:
 *     summary: Download trades Excel file for a wallet and market by date
 *     tags: [Wallets]
 *     parameters:
 *       - in: path
 *         name: walletName
 *         required: true
 *         schema:
 *           type: string
 *       - in: path
 *         name: market
 *         required: true
 *         schema:
 *           type: string
 *       - in: query
 *         name: date
 *         required: true
 *         schema:
 *           type: string
 *           format: date
 *           example: "2024-01-15"
 *         description: Date in YYYY-MM-DD format
 *     responses:
 *       200:
 *         description: Excel file with wallet trades for the specified date
 *         content:
 *           application/vnd.openxmlformats-officedocument.spreadsheetml.sheet:
 *             schema:
 *               type: string
 *               format: binary
 *       400:
 *         description: Date parameter is required
 *       404:
 *         description: Date file not found
 */
app.get(
  "/api/wallets/:walletName/markets/:market/download",
  async (req, res) => {
    try {
      const { walletName, market } = req.params;
      const { date } = req.query;

      // Date parameter is required
      if (!date || typeof date !== "string") {
        // If no date provided, return available dates
        const marketPath = path.join(WALLETS_DIR, walletName, market);

        try {
          await fs.access(marketPath);
        } catch {
          return res.status(404).json({ error: "Market folder not found" });
        }

        const files = await fs.readdir(marketPath);
        const dateFiles = files
          .filter((f) => f.endsWith(".xlsx"))
          .map((f) => f.replace(".xlsx", ""))
          .sort()
          .reverse();

        return res.status(400).json({
          error: "Date parameter is required. Use ?date=YYYY-MM-DD",
          availableDates: dateFiles,
          example: `/api/wallets/${walletName}/markets/${market}/download?date=${
            dateFiles[0] || "2024-01-15"
          }`,
        });
      }

      // Download specific date file
      const excelFile = path.join(
        WALLETS_DIR,
        walletName,
        market,
        `${date}.xlsx`
      );

      try {
        await fs.access(excelFile);
      } catch {
        return res
          .status(404)
          .json({ error: `Date file ${date}.xlsx not found` });
      }

      const workbook = XLSX.readFile(excelFile);

      if (workbook.SheetNames.length === 0) {
        return res.status(404).json({ error: "No data found in file" });
      }

      // Send the entire workbook (all sheets for that date)
      const excelBuffer = XLSX.write(workbook, {
        type: "buffer",
        bookType: "xlsx",
      });

      // Set headers for file download
      const fileName = `${walletName}-${market}-${date}.xlsx`;
      res.setHeader(
        "Content-Type",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
      );
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="${fileName}"`
      );
      res.setHeader("Content-Length", excelBuffer.length);

      res.send(excelBuffer);
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  }
);

/**
 * @swagger
 * /health:
 *   get:
 *     summary: Health check endpoint
 *     tags: [System]
 *     responses:
 *       200:
 *         description: Server is healthy
 */
app.get("/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

/**
 * Start the API server
 */
export function startApiServer(port: number = PORT) {
  return app.listen(port, () => {
    console.log(`🚀 API Server running on http://localhost:${port}`);
    console.log(`📊 Available endpoints:`);
    console.log(`   GET /api/wallets - List all wallets`);
    console.log(
      `   GET /api/wallets/:walletName/markets - Get markets for wallet`
    );
    console.log(
      `   GET /api/wallets/:walletName/markets/:market/dates - Get dates for market`
    );
    console.log(
      `   GET /api/wallets/:walletName/markets/:market/sessions - Get sessions`
    );
    console.log(
      `   GET /api/wallets/:walletName/markets/:market/sessions/:session - Get trades`
    );
    console.log(`   GET /api/markets - List all markets`);
    console.log(
      `   GET /api/markets/:slug/price-changes/download - Download price changes Excel`
    );
    console.log(
      `   GET /api/markets/:slug/price-changes-jsonl - Get price changes (JSONL)`
    );
    console.log(`   GET /api/markets/:slug/trades - Get trades`);
    console.log(
      `   GET /api/markets/:slug/orderbook-snapshot - Get orderbook snapshots`
    );
    console.log(
      `   GET /api/wallets/:walletName/markets/:market/download - Download wallet trades Excel`
    );
  });
}

// If running directly (not imported), start the server
if (require.main === module) {
  const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;
  console.log(
    `🚀 Starting API server as standalone process on port ${PORT}...`
  );
  const server = startApiServer(PORT);
  if (server) {
    server.on("error", (err: any) => {
      if (err.code === "EADDRINUSE") {
        console.error(
          `❌ Port ${PORT} is already in use. Please stop other processes or use a different port.`
        );
        process.exit(1);
      } else {
        console.error(`❌ API server error:`, err);
        process.exit(1);
      }
    });
  }
}
